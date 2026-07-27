<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient\Events;

use Deegitalbe\TrustupIoNotificationsContracts\Status\StatusPayload;

readonly class TrustupIoNotificationStatusReceived
{
    public function __construct(public StatusPayload $payload) {}
}
