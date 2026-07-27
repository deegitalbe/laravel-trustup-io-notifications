<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient\Exceptions;

use Deegitalbe\TrustupIoNotificationsClient\Contracts\SendsTrustupIoNotification;
use RuntimeException;

class MissingTrustupIoNotificationContractException extends RuntimeException
{
    public function __construct(string $notificationClass)
    {
        parent::__construct(
            "Notification [{$notificationClass}] must implement [".SendsTrustupIoNotification::class.'] to be sent through the TrustupIoNotifications channel.',
        );
    }
}
