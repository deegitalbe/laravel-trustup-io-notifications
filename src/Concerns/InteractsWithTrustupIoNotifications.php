<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient\Concerns;

trait InteractsWithTrustupIoNotifications
{
    /** @return list<\Deegitalbe\TrustupIoNotificationsContracts\Enums\NotificationChannel>|null */
    public function restrictTrustupIoNotificationsChannels(): ?array
    {
        return null;
    }
}
