<?php

declare(strict_types=1);

use Illuminate\Support\ServiceProvider;

it('publishes the client source config to the host app', function (): void {
    $paths = ServiceProvider::pathsToPublish(
        Deegitalbe\TrustupIoNotificationsClient\TrustupIoNotificationsClientServiceProvider::class,
        'trustup-io-notifications-client-config',
    );

    expect($paths)->not->toBeEmpty();

    $target = array_values($paths)[0];
    expect($target)->toEndWith('config/trustup-io-notifications.php');
});
