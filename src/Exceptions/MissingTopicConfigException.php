<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient\Exceptions;

use RuntimeException;

class MissingTopicConfigException extends RuntimeException
{
    public function __construct(string $configKey)
    {
        parent::__construct(
            "Required Kafka topic config key [{$configKey}] is not set. Publish this package's config and set the corresponding environment variable.",
        );
    }
}
