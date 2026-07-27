<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient\Channel;

use Deegitalbe\TrustupIoNotificationsClient\Actions\BuildNotificationRequest;
use Deegitalbe\TrustupIoNotificationsClient\Actions\PublishNotificationRequest;
use Deegitalbe\TrustupIoNotificationsClient\Contracts\SendsTrustupIoNotification;
use Deegitalbe\TrustupIoNotificationsClient\Exceptions\MissingTrustupIoNotificationContractException;
use Deegitalbe\TrustupIoNotificationsClient\Exceptions\UnresolvableRecipientException;
use Deegitalbe\TrustupIoNotificationsContracts\Request\Recipient;
use Illuminate\Notifications\AnonymousNotifiable;
use Illuminate\Notifications\Notification;

class TrustupIoNotificationsChannel
{
    public function __construct(
        private readonly BuildNotificationRequest $buildNotificationRequest,
        private readonly PublishNotificationRequest $publishNotificationRequest,
    ) {}

    public function send(object $notifiable, Notification $notification): void
    {
        if (! $notification instanceof SendsTrustupIoNotification) {
            throw new MissingTrustupIoNotificationContractException($notification::class);
        }

        $recipient = $this->resolveRecipient($notifiable, $notification);

        $data = $notification->toTrustupIoNotificationsData();

        $channels = $notification->restrictTrustupIoNotificationsChannels();

        $envelope = $this->buildNotificationRequest->execute($data, $recipient, $channels);

        $this->publishNotificationRequest->execute($envelope);
    }

    private function resolveRecipient(object $notifiable, Notification $notification): Recipient
    {
        if ($notifiable instanceof AnonymousNotifiable) {
            $recipient = $notifiable->routeNotificationFor('trustup-io-notifications');

            if (! $recipient instanceof Recipient) {
                throw new UnresolvableRecipientException($notifiable::class);
            }

            return $recipient;
        }

        if (! method_exists($notifiable, 'routeNotificationForTrustupIoNotifications')) {
            throw new UnresolvableRecipientException($notifiable::class);
        }

        $recipient = $notifiable->routeNotificationForTrustupIoNotifications($notification);

        if (! $recipient instanceof Recipient) {
            throw new UnresolvableRecipientException($notifiable::class);
        }

        return $recipient;
    }
}
