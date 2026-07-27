<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient\Events;

use Deegitalbe\TrustupIoNotificationsContracts\Engagement\EngagementPayload;

readonly class TrustupIoNotificationEngagementReceived
{
    public function __construct(public EngagementPayload $payload) {}
}
