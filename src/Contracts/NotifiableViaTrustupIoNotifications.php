<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient\Contracts;

use Deegitalbe\TrustupIoNotificationsContracts\Request\Recipient;
use Illuminate\Notifications\Notification;

interface NotifiableViaTrustupIoNotifications
{
    public function routeNotificationForTrustupIoNotifications(?Notification $notification = null): Recipient;

    public function toTrustupIoNotificationsRecipient(): Recipient;
}
