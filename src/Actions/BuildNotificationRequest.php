<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient\Actions;

use Deegitalbe\TrustupIoNotificationsClient\Exceptions\MissingSourceException;
use Deegitalbe\TrustupIoNotificationsContracts\Contracts\NotificationData;
use Deegitalbe\TrustupIoNotificationsContracts\Enums\EventDirection;
use Deegitalbe\TrustupIoNotificationsContracts\Enums\Source;
use Deegitalbe\TrustupIoNotificationsContracts\Envelope;
use Deegitalbe\TrustupIoNotificationsContracts\Request\Recipient;
use Deegitalbe\TrustupIoNotificationsContracts\Request\RequestPayload;
use Illuminate\Support\Str;

class BuildNotificationRequest
{
    /** @param list<\Deegitalbe\TrustupIoNotificationsContracts\Enums\NotificationChannel>|null $channels */
    public function execute(NotificationData $data, Recipient $recipient, ?array $channels): Envelope
    {
        $recipient = $this->resolveSource($recipient);

        $type = $data->notificationType();

        $payload = new RequestPayload(
            type: $type,
            recipient: $recipient,
            data: $data,
            channels: $channels,
        );

        return new Envelope(
            version: Envelope::CURRENT_VERSION,
            direction: EventDirection::Request,
            payload: $payload,
            eventId: (string) Str::ulid(),
        );
    }

    private function resolveSource(Recipient $recipient): Recipient
    {
        if ($recipient->source !== null) {
            return $recipient;
        }

        $configSource = config('trustup-io-notifications.source');

        if ($configSource === null) {
            throw new MissingSourceException;
        }

        return $recipient->withSource(Source::from((string) $configSource));
    }
}
