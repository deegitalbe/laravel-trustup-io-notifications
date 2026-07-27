# Trustup IO Notifications (Laravel client)

Drop-in Laravel client for the Trustup IO Notifications service. It lets a source application **send** notifications through Laravel's native notification system and **receive** delivery feedback (delivered, bounced, opened, clicked, ...), all over Kafka.

- Package name: `deegitalbe/laravel-trustup-io-notifications`
- Namespace: `Deegitalbe\TrustupIoNotificationsClient\`
- Requires: PHP 8.2+, Laravel 12, `mateusjunges/laravel-kafka`, and the contracts package `deegitalbe/laravel-trustup-io-notifications-contracts`.

The notification payloads and channel definitions live in the contracts package. See its README for how to define a `NotificationData` class, override channel rendering, and add a new notification type. This README covers wiring the client into an app and the send / receive flows.

---

## 1. Install

```bash
composer require deegitalbe/laravel-trustup-io-notifications
```

The service provider is auto-discovered. It registers the config file and the two consume commands.

## 2. Configure

Publish the config if you want to edit defaults (optional; env vars are enough for most apps):

```bash
php artisan vendor:publish --provider="Deegitalbe\TrustupIoNotificationsClient\TrustupIoNotificationsClientServiceProvider"
```

Set the source and the Kafka broker. Everything else has working defaults.

```dotenv
# Which source this app is (tools | marketplace). Used as the default recipient source.
TRUSTUP_IO_NOTIFICATIONS_SOURCE=tools

# Kafka broker (mateusjunges/laravel-kafka config)
KAFKA_BROKERS=kafka:9092
```

Config keys (`config/trustup-io-notifications.php`):

| Key | Env | Default | Purpose |
| --- | --- | --- | --- |
| `topics.request` | `TRUSTUP_IO_NOTIFICATIONS_TOPIC_REQUEST` | `notifications.request` | Topic this app **publishes** send requests to. |
| `topics.status` | `TRUSTUP_IO_NOTIFICATIONS_TOPIC_STATUS` | `notifications.status` | Topic this app **consumes** delivery statuses from. |
| `topics.engagement` | `TRUSTUP_IO_NOTIFICATIONS_TOPIC_ENGAGEMENT` | `notifications.engagement` | Topic this app **consumes** engagement events from. |
| `topics.dlq` | `TRUSTUP_IO_NOTIFICATIONS_TOPIC_DLQ` | `notifications.dlq` | Dead-letter topic for both consumers. |
| `source` | `TRUSTUP_IO_NOTIFICATIONS_SOURCE` | `null` | Default `Source` when a recipient has none. If null and no source on the recipient → `MissingSourceException`. |

## 3. Send a notification (bare minimum)

Three pieces: a **notification** class, a **notifiable** that knows its recipient, and a `notify()` call.

### The notification

The notification receives your own domain models and derives the `NotificationData` from them inside `toTrustupIoNotificationsData()`. You do not pass the data object in from the outside.

```php
use App\Models\Comment;
use Illuminate\Notifications\Notification;
use Deegitalbe\TrustupIoNotificationsClient\Channel\TrustupIoNotificationsChannel;
use Deegitalbe\TrustupIoNotificationsClient\Concerns\InteractsWithTrustupIoNotifications;
use Deegitalbe\TrustupIoNotificationsClient\Contracts\SendsTrustupIoNotification;
use Deegitalbe\TrustupIoNotificationsContracts\Contracts\NotificationData;
use Deegitalbe\TrustupIoNotificationsContracts\Data\ToolsCommentNotificationData;

class CommentNotification extends Notification implements SendsTrustupIoNotification
{
    use InteractsWithTrustupIoNotifications;

    public function __construct(private Comment $comment) {}

    public function via(object $notifiable): array
    {
        return [TrustupIoNotificationsChannel::class];
    }

    public function toTrustupIoNotificationsData(): NotificationData
    {
        return new ToolsCommentNotificationData(
            product_url: $this->comment->product->url,
            product_name: $this->comment->product->name,
            body: $this->comment->body,
            attachment_details: $this->comment->attachmentDetails(),
            commenter_name: $this->comment->author->name,
            timestamp: $this->comment->created_at->toIso8601String(),
            action_url: $this->comment->url,
            notifications_url: route('notifications.index'),
            company_name: $this->comment->product->company->name,
            company_address: $this->comment->product->company->address,
        );
    }
}
```

- Implement `SendsTrustupIoNotification`; `toTrustupIoNotificationsData()` maps your models to the typed payload.
- `use InteractsWithTrustupIoNotifications` to get the default "all channels" behavior; without it you must implement `restrictTrustupIoNotificationsChannels()` yourself.
- Reference the channel by class in `via()`.
- The `NotificationData` class (`ToolsCommentNotificationData` here) comes from the contracts package; see its README to define your own.

### The notifiable

Add Laravel's `Notifiable` (for `notify()`), implement the interface, and use the routing trait:

```php
use Illuminate\Notifications\Notifiable;
use Deegitalbe\TrustupIoNotificationsClient\Concerns\RoutesTrustupIoNotifications;
use Deegitalbe\TrustupIoNotificationsClient\Contracts\NotifiableViaTrustupIoNotifications;
use Deegitalbe\TrustupIoNotificationsContracts\Enums\Source;
use Deegitalbe\TrustupIoNotificationsContracts\Request\Recipient;

class User extends Model implements NotifiableViaTrustupIoNotifications
{
    use Notifiable;
    use RoutesTrustupIoNotifications;

    public function toTrustupIoNotificationsRecipient(): Recipient
    {
        return Recipient::identified((string) $this->id);
    }
}
```

`Notifiable` is required (it provides `notify()`); the package does not include it for you. `RoutesTrustupIoNotifications` wires Laravel's routing convention to your `toTrustupIoNotificationsRecipient()`.

The recipient omits the source: the client fills it from `TRUSTUP_IO_NOTIFICATIONS_SOURCE` at publish time. Pass it explicitly (`Recipient::identified((string) $this->id, Source::Tools)`) only if this app sends for more than one source.

### Send it

```php
$user->notify(new CommentNotification($comment));
```

That publishes the request to Kafka. The notifications service picks it up, resolves the recipient's coordinates, and delivers on each supported channel.

## 4. Receive delivery feedback

The service publishes back onto the `status` and `engagement` topics. The client turns those into two Laravel events you listen to.

### Run the consumers + a queue worker

The consume commands are long-running Kafka consumers. They only **enqueue** jobs; the events fire from inside those queued jobs. So you need the consumers **and** a queue worker running:

```bash
php artisan trustup-io-notifications:consume-statuses
php artisan trustup-io-notifications:consume-engagements
php artisan queue:work            # or Horizon
```

Without a queue worker, statuses and engagements are consumed but the events never dispatch.

### Listen to the events

```php
use Illuminate\Support\Facades\Event;
use Deegitalbe\TrustupIoNotificationsClient\Events\TrustupIoNotificationStatusReceived;
use Deegitalbe\TrustupIoNotificationsClient\Events\TrustupIoNotificationEngagementReceived;

Event::listen(TrustupIoNotificationStatusReceived::class, function ($event) {
    $event->payload->sendId;         // string
    $event->payload->channel;        // NotificationChannel: email|sms|push
    $event->payload->status;         // NotificationStatus: pending|sent|delivered|error
    $event->payload->type;           // NotificationType
    $event->payload->data;           // hydrated NotificationData
});

Event::listen(TrustupIoNotificationEngagementReceived::class, function ($event) {
    $event->payload->kind;           // ChannelEventKind: delivered|bounced|spam_complaint|opened|clicked|...
    $event->payload->clickedUrl;     // ?string (set for clicks)
    $event->payload->sendId;
    $event->payload->channel;
});
```

The package defines **no listeners of its own** by design: it emits the events, your app decides what to do (update a local model, alert on bounce, track engagement, ...). Register as many listeners as you want, on either event. Dispatching with zero listeners is a no-op, not an error.

`StatusPayload` and `EngagementPayload` shapes are documented in the contracts README.

## 5. What must be running

| Process | Role | Where |
| --- | --- | --- |
| `queue:work` / Horizon | Runs the queued jobs that both send and receive rely on | source app |
| `trustup-io-notifications:consume-statuses` | Consumes delivery statuses → `TrustupIoNotificationStatusReceived` | source app |
| `trustup-io-notifications:consume-engagements` | Consumes engagements → `TrustupIoNotificationEngagementReceived` | source app |
| Kafka broker | Transport for all topics | shared infra |
| The notifications service | Consumes `request`, delivers, publishes `status` / `engagement` | separate service |

Sending only needs a reachable Kafka broker. Receiving feedback needs the two consumers **and** a queue worker.

---

## Customization

### Restrict channels per notification

By default a notification goes out on every channel its data class supports. Override to narrow it:

```php
use Deegitalbe\TrustupIoNotificationsContracts\Enums\NotificationChannel;

public function restrictTrustupIoNotificationsChannels(): ?array
{
    return [NotificationChannel::Email]; // email only, even if the data class also supports sms/push
}
```

`null` = all supported channels. `[]` is invalid and rejected (`InvalidEnvelopeException`).

### Anonymous recipients (no model)

When there is no notifiable model, route on the fly with a `Recipient::anonymous(...)`:

```php
use Illuminate\Support\Facades\Notification;
use Deegitalbe\TrustupIoNotificationsContracts\Request\Recipient;

Notification::route('trustup-io-notifications',
    Recipient::anonymous(email: 'a@b.com', phone: null, deviceTokens: [], locale: 'fr-BE')
)->notify(new CommentNotification($comment));
```

The routing key is the literal string `trustup-io-notifications`.

### Customize the payload / channels / rendering

The notification's **content** (which template, which variables, SMS/push wording, translations) is controlled by the `NotificationData` class and its `Renders*` traits, all in the contracts package. See that README's "Capability interfaces and their default rendering" section for the overridable methods (`emailTemplate`, `emailVariables`, `emailTemplateLocaleGranularity`, `smsBody`, `pushTitle`, ...).

### Default source vs explicit source

`Recipient::identified($id, Source::Tools)` sets the source explicitly. Built without a source (`Recipient::identified($id)` or any anonymous recipient), the client fills it from `config('trustup-io-notifications.source')` at publish time. Set `TRUSTUP_IO_NOTIFICATIONS_SOURCE` so this always resolves.

## Errors

| Exception | When |
| --- | --- |
| `MissingTrustupIoNotificationContractException` | The notification does not implement `SendsTrustupIoNotification`. |
| `UnresolvableRecipientException` | The notifiable exposes no recipient (missing method / on-demand route not a `Recipient`). |
| `MissingSourceException` | Recipient has no source and no `config('...source')` default. |
| `MissingTopicConfigException` | A required topic config key is null at publish time. |
| `InvalidRecipientException` / `InvalidEnvelopeException` | Invalid recipient coordinates or an empty channel list. |
