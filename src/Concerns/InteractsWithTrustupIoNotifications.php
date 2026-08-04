<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient\Concerns;

trait InteractsWithTrustupIoNotifications
{
    /**
     * The default restriction is "no restriction", which is genuinely independent of the
     * recipient. The parameter exists only to satisfy the interface signature.
     *
     * @return list<\Deegitalbe\TrustupIoNotificationsContracts\Enums\NotificationChannel>|null
     */
    public function restrictTrustupIoNotificationsChannels(object $notifiable): ?array
    {
        return null;
    }
}
