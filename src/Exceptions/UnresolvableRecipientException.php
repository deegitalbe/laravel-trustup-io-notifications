<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient\Exceptions;

use RuntimeException;

class UnresolvableRecipientException extends RuntimeException
{
    public function __construct(string $notifiableClass)
    {
        parent::__construct(
            "Could not resolve recipient for notifiable [{$notifiableClass}]. Define [routeNotificationForTrustupIoNotifications] on the model or use [Notification::route('trustup-io-notifications', ...)] for on-demand notifications.",
        );
    }
}
