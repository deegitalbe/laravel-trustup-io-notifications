<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient\Jobs;

use Deegitalbe\TrustupIoNotificationsClient\Events\TrustupIoNotificationStatusReceived;
use Deegitalbe\TrustupIoNotificationsContracts\Status\StatusPayload;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;

class HandleNotificationStatusJob implements ShouldQueue
{
    use Dispatchable;
    use InteractsWithQueue;
    use Queueable;
    use SerializesModels;

    public function __construct(public readonly StatusPayload $payload) {}

    public function handle(): void
    {
        event(new TrustupIoNotificationStatusReceived($this->payload));
    }
}
