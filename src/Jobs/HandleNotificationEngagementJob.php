<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient\Jobs;

use Deegitalbe\TrustupIoNotificationsClient\Events\TrustupIoNotificationEngagementReceived;
use Deegitalbe\TrustupIoNotificationsContracts\Engagement\EngagementPayload;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;

class HandleNotificationEngagementJob implements ShouldQueue
{
    use Dispatchable;
    use InteractsWithQueue;
    use Queueable;
    use SerializesModels;

    public function __construct(public readonly EngagementPayload $payload) {}

    public function handle(): void
    {
        event(new TrustupIoNotificationEngagementReceived($this->payload));
    }
}
