<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient\Contracts;

use Deegitalbe\TrustupIoNotificationsContracts\Contracts\NotificationData;
use Deegitalbe\TrustupIoNotificationsContracts\Enums\NotificationChannel;

interface SendsTrustupIoNotification
{
    public function toTrustupIoNotificationsData(object $notifiable): NotificationData;

    /** @return list<NotificationChannel>|null */
    public function restrictTrustupIoNotificationsChannels(object $notifiable): ?array;
}
