<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient;

use Deegitalbe\TrustupIoNotificationsClient\Console\ConsumeEngagementsCommand;
use Deegitalbe\TrustupIoNotificationsClient\Console\ConsumeStatusesCommand;
use Spatie\LaravelPackageTools\Package;
use Spatie\LaravelPackageTools\PackageServiceProvider;

class TrustupIoNotificationsClientServiceProvider extends PackageServiceProvider
{
    public function configurePackage(Package $package): void
    {
        $package
            ->name('trustup-io-notifications-client')
            ->hasConfigFile('trustup-io-notifications')
            ->hasCommands([
                ConsumeStatusesCommand::class,
                ConsumeEngagementsCommand::class,
            ]);
    }
}
