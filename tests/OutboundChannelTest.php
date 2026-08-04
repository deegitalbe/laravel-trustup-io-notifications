<?php

declare(strict_types=1);

use Deegitalbe\TrustupIoNotificationsClient\Channel\TrustupIoNotificationsChannel;
use Deegitalbe\TrustupIoNotificationsClient\Concerns\InteractsWithTrustupIoNotifications;
use Deegitalbe\TrustupIoNotificationsClient\Contracts\SendsTrustupIoNotification;
use Deegitalbe\TrustupIoNotificationsClient\Exceptions\MissingTopicConfigException;
use Deegitalbe\TrustupIoNotificationsClient\Exceptions\MissingTrustupIoNotificationContractException;
use Deegitalbe\TrustupIoNotificationsClient\Exceptions\UnresolvableRecipientException;
use Deegitalbe\TrustupIoNotificationsContracts\Contracts\NotificationData;
use Deegitalbe\TrustupIoNotificationsContracts\Data\ToolsTestNotificationData;
use Deegitalbe\TrustupIoNotificationsContracts\Enums\NotificationChannel;
use Deegitalbe\TrustupIoNotificationsContracts\Enums\NotificationType;
use Deegitalbe\TrustupIoNotificationsContracts\Enums\Source;
use Deegitalbe\TrustupIoNotificationsContracts\Exceptions\InvalidEnvelopeException;
use Deegitalbe\TrustupIoNotificationsContracts\Exceptions\InvalidRecipientException;
use Deegitalbe\TrustupIoNotificationsContracts\Request\Recipient;
use Illuminate\Notifications\AnonymousNotifiable;
use Illuminate\Notifications\Notification;
use Junges\Kafka\Facades\Kafka;

// -------------------------------------------------------------------------
// Helpers: minimal notifiable and notification classes used in multiple tests
// -------------------------------------------------------------------------

function makeIdentifiedNotifiable(): object
{
    return new class
    {
        public function routeNotificationForTrustupIoNotifications(Notification $notification): Recipient
        {
            return Recipient::identified('user-42', Source::Tools);
        }
    };
}

/**
 * @param  list<NotificationChannel>|null|'default'  $restrictedChannels  Pass 'default' to use the null default (no restriction).
 */
function makeConformingNotification(
    mixed $restrictedChannels = 'default',
): Notification {
    return new class($restrictedChannels) extends Notification implements SendsTrustupIoNotification
    {
        public function __construct(private readonly mixed $restrictedChannels = 'default') {}

        public function via(object $notifiable): array
        {
            return [TrustupIoNotificationsChannel::class];
        }

        public function toTrustupIoNotificationsData(object $notifiable): NotificationData
        {
            return new ToolsTestNotificationData('https://example.test', 'Test Title', 'Test Body');
        }

        public function restrictTrustupIoNotificationsChannels(object $notifiable): ?array
        {
            if ($this->restrictedChannels === 'default') {
                return null;
            }

            return $this->restrictedChannels;
        }
    };
}

/**
 * Notifiable carrying a title, so a notification can derive a per-recipient payload from it.
 */
function makeTitledNotifiable(string $title): object
{
    return new class($title)
    {
        public function __construct(public readonly string $title) {}

        public function routeNotificationForTrustupIoNotifications(Notification $notification): Recipient
        {
            return Recipient::identified('user-42', Source::Tools);
        }
    };
}

/**
 * Notifiable carrying its own allowed channel list, so a notification can derive a
 * per-recipient restriction from it.
 *
 * @param  list<NotificationChannel>  $allowedChannels
 */
function makeRestrictingNotifiable(array $allowedChannels): object
{
    return new class($allowedChannels)
    {
        /** @param list<NotificationChannel> $allowedChannels */
        public function __construct(public readonly array $allowedChannels) {}

        public function routeNotificationForTrustupIoNotifications(Notification $notification): Recipient
        {
            return Recipient::identified('user-42', Source::Tools);
        }
    };
}

// -------------------------------------------------------------------------
// Cycle 1: Payload varies by notifiable (AC-1, AC-3)
// -------------------------------------------------------------------------
it('publishes the payload derived from the notifiable it was sent to when the notification varies its data per notifiable', function (): void {
    Kafka::fake();

    $notifiableA = makeTitledNotifiable('Title for A');
    $notification = new class extends Notification implements SendsTrustupIoNotification
    {
        use InteractsWithTrustupIoNotifications;

        public function via(object $notifiable): array
        {
            return [TrustupIoNotificationsChannel::class];
        }

        public function toTrustupIoNotificationsData(object $notifiable): NotificationData
        {
            return new ToolsTestNotificationData('https://example.test', $notifiable->title, 'Test Body');
        }
    };

    app(TrustupIoNotificationsChannel::class)->send($notifiableA, $notification);

    Kafka::assertPublishedOn(
        topic: config('trustup-io-notifications.topics.request'),
        callback: function ($message): bool {
            /** @var array<string, mixed> $body */
            $body = (array) $message->getBody();

            return ($body['payload']['data'] ?? null) === [
                'base_url' => 'https://example.test',
                'title' => 'Title for A',
                'body' => 'Test Body',
            ];
        },
    );

    Kafka::assertPublishedTimes(1);
});

// -------------------------------------------------------------------------
// Cycle 2: Channel restriction varies by notifiable (AC-2, AC-4)
// -------------------------------------------------------------------------
it('publishes the channel restriction derived from the notifiable it was sent to when the notification varies its restriction per notifiable', function (): void {
    Kafka::fake();

    $smsOnlyNotifiable = makeRestrictingNotifiable([NotificationChannel::Sms]);
    $notification = new class extends Notification implements SendsTrustupIoNotification
    {
        public function via(object $notifiable): array
        {
            return [TrustupIoNotificationsChannel::class];
        }

        public function toTrustupIoNotificationsData(object $notifiable): NotificationData
        {
            return new ToolsTestNotificationData('https://example.test', 'Test Title', 'Test Body');
        }

        public function restrictTrustupIoNotificationsChannels(object $notifiable): ?array
        {
            return $notifiable->allowedChannels;
        }
    };

    app(TrustupIoNotificationsChannel::class)->send($smsOnlyNotifiable, $notification);

    Kafka::assertPublishedOn(
        topic: config('trustup-io-notifications.topics.request'),
        callback: function ($message): bool {
            /** @var array<string, mixed> $body */
            $body = (array) $message->getBody();

            return ($body['payload']['channels'] ?? null) === ['sms'];
        },
    );

    Kafka::assertPublishedTimes(1);
});

// -------------------------------------------------------------------------
// Cycle 3: Anonymous path forwards the AnonymousNotifiable instance (AC-5)
// -------------------------------------------------------------------------
it('passes the same AnonymousNotifiable instance to both contract methods and still publishes on the anonymous path', function (): void {
    Kafka::fake();

    config(['trustup-io-notifications.source' => Source::Tools->value]);

    $notification = new class extends Notification implements SendsTrustupIoNotification
    {
        public ?object $dataNotifiable = null;

        public ?object $restrictionNotifiable = null;

        public function via(object $notifiable): array
        {
            return [TrustupIoNotificationsChannel::class];
        }

        public function toTrustupIoNotificationsData(object $notifiable): NotificationData
        {
            $this->dataNotifiable = $notifiable;

            return new ToolsTestNotificationData('https://example.test', 'Test Title', 'Test Body');
        }

        public function restrictTrustupIoNotificationsChannels(object $notifiable): ?array
        {
            $this->restrictionNotifiable = $notifiable;

            return null;
        }
    };

    $anonymousNotifiable = new AnonymousNotifiable;
    $anonymousNotifiable->route('trustup-io-notifications', Recipient::anonymous('anon@example.com', null, [], locale: 'fr-BE'));

    app(TrustupIoNotificationsChannel::class)->send($anonymousNotifiable, $notification);

    expect($notification->dataNotifiable)->toBe($anonymousNotifiable)
        ->and($notification->restrictionNotifiable)->toBe($anonymousNotifiable);

    Kafka::assertPublishedTimes(1);
});

// -------------------------------------------------------------------------
// Cycle 4: Nominal identified recipient, no restriction
// -------------------------------------------------------------------------
it('publishes one event to the request topic with correct fields when sent to an identified notifiable', function (): void {
    Kafka::fake();

    $notifiable = makeIdentifiedNotifiable();
    $notification = makeConformingNotification();

    app(TrustupIoNotificationsChannel::class)->send($notifiable, $notification);

    Kafka::assertPublishedOn(
        topic: config('trustup-io-notifications.topics.request'),
        callback: function ($message): bool {
            /** @var array<string, mixed> $body */
            $body = (array) $message->getBody();

            return ($body['version'] ?? null) === 1
                && ($body['direction'] ?? null) === 'request'
                && ($body['event_id'] ?? null) !== null
                && ($body['payload']['type'] ?? null) === NotificationType::ToolsTestNotification->value
                && ($body['payload']['recipient']['identified'] ?? null) === true
                && ($body['payload']['recipient']['source'] ?? null) === Source::Tools->value
                && ($body['payload']['recipient']['external_user_id'] ?? null) === 'user-42'
                && ($body['payload']['data'] ?? null) === ['base_url' => 'https://example.test', 'title' => 'Test Title', 'body' => 'Test Body']
                && array_key_exists('channels', (array) ($body['payload'] ?? []))
                && ($body['payload']['channels'] === null);
        },
    );

    Kafka::assertPublishedTimes(1);
});

// -------------------------------------------------------------------------
// Cycle 5: Anonymous on-demand recipient (AC-4) — behavior already implemented
// -------------------------------------------------------------------------
it('publishes one event with anonymous recipient shape when sent via Notification::route on-demand', function (): void {
    Kafka::fake();

    config(['trustup-io-notifications.source' => Source::Tools->value]);

    $notification = makeConformingNotification();

    $anonymousNotifiable = new AnonymousNotifiable;
    $anonymousNotifiable->route('trustup-io-notifications', Recipient::anonymous('anon@example.com', null, [], locale: 'fr-BE'));

    app(TrustupIoNotificationsChannel::class)->send($anonymousNotifiable, $notification);

    Kafka::assertPublishedOn(
        topic: config('trustup-io-notifications.topics.request'),
        callback: function ($message): bool {
            /** @var array<string, mixed> $body */
            $body = (array) $message->getBody();

            return ($body['payload']['recipient']['identified'] ?? null) === false
                && ($body['payload']['recipient']['email'] ?? null) === 'anon@example.com'
                && array_key_exists('phone', (array) ($body['payload']['recipient'] ?? []))
                && ($body['payload']['recipient']['phone'] === null);
        },
    );

    Kafka::assertPublishedTimes(1);
});

// -------------------------------------------------------------------------
// Cycle 6: Channel restriction (AC-7)
// -------------------------------------------------------------------------
it('publishes with channels set to [sms] when notification restricts to Sms channel', function (): void {
    Kafka::fake();

    $notifiable = makeIdentifiedNotifiable();
    $notification = makeConformingNotification([NotificationChannel::Sms]);

    app(TrustupIoNotificationsChannel::class)->send($notifiable, $notification);

    Kafka::assertPublishedOn(
        topic: config('trustup-io-notifications.topics.request'),
        callback: function ($message): bool {
            /** @var array<string, mixed> $body */
            $body = (array) $message->getBody();

            return ($body['payload']['channels'] ?? null) === ['sms'];
        },
    );
});

// -------------------------------------------------------------------------
// Cycle 7: Empty channel restriction (AC-7, AC-10)
// -------------------------------------------------------------------------
it('throws InvalidEnvelopeException and publishes nothing when channel restriction is an empty array', function (): void {
    Kafka::fake();

    $notifiable = makeIdentifiedNotifiable();
    $notification = makeConformingNotification([]);

    expect(fn () => app(TrustupIoNotificationsChannel::class)->send($notifiable, $notification))
        ->toThrow(InvalidEnvelopeException::class);

    Kafka::assertNothingPublished();
});

// -------------------------------------------------------------------------
// Cycle 8: Missing contract exception (AC-1)
// -------------------------------------------------------------------------
it('throws MissingTrustupIoNotificationContractException when notification does not implement SendsTrustupIoNotification', function (): void {
    Kafka::fake();

    $notifiable = makeIdentifiedNotifiable();
    $notification = new class extends Notification
    {
        public function via(object $notifiable): array
        {
            return [TrustupIoNotificationsChannel::class];
        }
    };

    expect(fn () => app(TrustupIoNotificationsChannel::class)->send($notifiable, $notification))
        ->toThrow(MissingTrustupIoNotificationContractException::class);

    Kafka::assertNothingPublished();
});

// -------------------------------------------------------------------------
// Cycle 9: Unresolvable recipient (AC-3)
// -------------------------------------------------------------------------
it('throws UnresolvableRecipientException when notifiable defines no routing method and no on-demand route is provided', function (): void {
    Kafka::fake();

    $notifiable = new class {};
    $notification = makeConformingNotification();

    expect(fn () => app(TrustupIoNotificationsChannel::class)->send($notifiable, $notification))
        ->toThrow(UnresolvableRecipientException::class);

    Kafka::assertNothingPublished();
});

// -------------------------------------------------------------------------
// Cycle 10: Anonymous with no coordinates (AC-4, AC-10)
// -------------------------------------------------------------------------
it('throws InvalidRecipientException when anonymous recipient is created with no coordinates', function (): void {
    Kafka::fake();

    expect(fn () => Recipient::anonymous(null, null, []))
        ->toThrow(InvalidRecipientException::class);

    Kafka::assertNothingPublished();
});

// -------------------------------------------------------------------------
// Cycle 11: Missing topic config (AC-11)
// -------------------------------------------------------------------------
it('throws MissingTopicConfigException when topics request config is null at send time', function (): void {
    Kafka::fake();

    config(['trustup-io-notifications.topics.request' => null]);

    $notifiable = makeIdentifiedNotifiable();
    $notification = makeConformingNotification();

    expect(fn () => app(TrustupIoNotificationsChannel::class)->send($notifiable, $notification))
        ->toThrow(MissingTopicConfigException::class);

    Kafka::assertNothingPublished();
});

// -------------------------------------------------------------------------
// Cycle 18: Notification using the trait publishes with null channels (trait coverage)
// -------------------------------------------------------------------------
it('publishes with null channels when notification uses InteractsWithTrustupIoNotifications trait default', function (): void {
    Kafka::fake();

    $notifiable = makeIdentifiedNotifiable();
    $notification = new class extends Notification implements SendsTrustupIoNotification
    {
        use InteractsWithTrustupIoNotifications;

        public function via(object $notifiable): array
        {
            return [TrustupIoNotificationsChannel::class];
        }

        public function toTrustupIoNotificationsData(object $notifiable): NotificationData
        {
            return new ToolsTestNotificationData('https://example.test', 'Title', 'Body');
        }
    };

    app(TrustupIoNotificationsChannel::class)->send($notifiable, $notification);

    Kafka::assertPublishedOn(
        topic: config('trustup-io-notifications.topics.request'),
        callback: function ($message): bool {
            /** @var array<string, mixed> $body */
            $body = (array) $message->getBody();
            /** @var array<string, mixed> $payload */
            $payload = (array) ($body['payload'] ?? []);

            return array_key_exists('channels', $payload) && $payload['channels'] === null;
        },
    );
});

// -------------------------------------------------------------------------
// Cycle 19: AnonymousNotifiable whose route returns non-Recipient throws (line 48 coverage)
// -------------------------------------------------------------------------
it('throws UnresolvableRecipientException when anonymous notifiable route returns a non-Recipient value', function (): void {
    Kafka::fake();

    $notification = makeConformingNotification();

    $anonymousNotifiable = new AnonymousNotifiable;
    $anonymousNotifiable->route('trustup-io-notifications', 'not-a-recipient');

    expect(fn () => app(TrustupIoNotificationsChannel::class)->send($anonymousNotifiable, $notification))
        ->toThrow(UnresolvableRecipientException::class);

    Kafka::assertNothingPublished();
});

// -------------------------------------------------------------------------
// Cycle 20: Notifiable whose routing method returns non-Recipient throws (line 61 coverage)
// -------------------------------------------------------------------------
it('throws UnresolvableRecipientException when routeNotificationForTrustupIoNotifications returns a non-Recipient value', function (): void {
    Kafka::fake();

    $notifiable = new class
    {
        public function routeNotificationForTrustupIoNotifications(Notification $notification): string
        {
            return 'not-a-recipient';
        }
    };
    $notification = makeConformingNotification();

    expect(fn () => app(TrustupIoNotificationsChannel::class)->send($notifiable, $notification))
        ->toThrow(UnresolvableRecipientException::class);

    Kafka::assertNothingPublished();
});
