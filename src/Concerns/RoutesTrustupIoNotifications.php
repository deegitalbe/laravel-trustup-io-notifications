<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient\Concerns;

use Deegitalbe\TrustupIoNotificationsContracts\Request\Recipient;
use Illuminate\Notifications\Notification;

/**
 * @mixin \Deegitalbe\TrustupIoNotificationsClient\Contracts\NotifiableViaTrustupIoNotifications
 */
trait RoutesTrustupIoNotifications
{
    public function routeNotificationForTrustupIoNotifications(?Notification $notification = null): Recipient
    {
        return $this->toTrustupIoNotificationsRecipient();
    }
}
