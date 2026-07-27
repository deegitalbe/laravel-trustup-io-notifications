<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient\Exceptions;

use RuntimeException;

class MissingSourceException extends RuntimeException
{
    public function __construct()
    {
        parent::__construct(
            'Notification source could not be resolved. Set a source on the Recipient or configure TRUSTUP_IO_NOTIFICATIONS_SOURCE.',
        );
    }
}
