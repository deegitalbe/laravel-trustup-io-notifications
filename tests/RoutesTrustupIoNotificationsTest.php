<?php

declare(strict_types=1);

use Deegitalbe\TrustupIoNotificationsClient\Channel\TrustupIoNotificationsChannel;
use Deegitalbe\TrustupIoNotificationsClient\Concerns\InteractsWithTrustupIoNotifications;
use Deegitalbe\TrustupIoNotificationsClient\Concerns\RoutesTrustupIoNotifications;
use Deegitalbe\TrustupIoNotificationsClient\Contracts\NotifiableViaTrustupIoNotifications;
use Deegitalbe\TrustupIoNotificationsClient\Contracts\SendsTrustupIoNotification;
use Deegitalbe\TrustupIoNotificationsContracts\Contracts\NotificationData;
use Deegitalbe\TrustupIoNotificationsContracts\Data\ToolsTestNotificationData;
use Deegitalbe\TrustupIoNotificationsContracts\Enums\Source;
use Deegitalbe\TrustupIoNotificationsContracts\Request\Recipient;
use Illuminate\Notifications\Notifiable;
use Illuminate\Notifications\Notification;
use Junges\Kafka\Facades\Kafka;

function makeTraitNotification(): Notification
{
    return new class extends Notification implements SendsTrustupIoNotification
    {
        use InteractsWithTrustupIoNotifications;

        /** @return list<class-string> */
        public function via(object $notifiable): array
        {
            return [TrustupIoNotificationsChannel::class];
        }

        public function toTrustupIoNotificationsData(): NotificationData
        {
            return new ToolsTestNotificationData('https://example.test', 'Title', 'Body');
        }
    };
}

it('routes to the recipient returned by toTrustupIoNotificationsRecipient when using the trait', function (): void {
    Kafka::fake();

    $notifiable = new class implements NotifiableViaTrustupIoNotifications
    {
        use Notifiable;
        use RoutesTrustupIoNotifications;

        public function toTrustupIoNotificationsRecipient(): Recipient
        {
            return Recipient::identified('user-99', Source::Tools);
        }
    };

    app(TrustupIoNotificationsChannel::class)->send($notifiable, makeTraitNotification());

    Kafka::assertPublishedOn(
        topic: config('trustup-io-notifications.topics.request'),
        callback: function ($message): bool {
            /** @var array<string, mixed> $body */
            $body = (array) $message->getBody();

            return ($body['payload']['recipient']['identified'] ?? null) === true
                && ($body['payload']['recipient']['source'] ?? null) === Source::Tools->value
                && ($body['payload']['recipient']['external_user_id'] ?? null) === 'user-99';
        },
    );

    Kafka::assertPublishedTimes(1);
});

it('returns the recipient directly when the routing method is called', function (): void {
    $notifiable = new class implements NotifiableViaTrustupIoNotifications
    {
        use Notifiable;
        use RoutesTrustupIoNotifications;

        public function toTrustupIoNotificationsRecipient(): Recipient
        {
            return Recipient::identified('user-1', Source::Tools);
        }
    };

    $recipient = $notifiable->routeNotificationForTrustupIoNotifications();

    expect($recipient)->toBeInstanceOf(Recipient::class)
        ->and($recipient->externalUserId)->toBe('user-1')
        ->and($recipient->source)->toBe(Source::Tools);
});
