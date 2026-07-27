<?php

declare(strict_types=1);

use Deegitalbe\TrustupIoNotificationsClient\Actions\BuildNotificationRequest;
use Deegitalbe\TrustupIoNotificationsClient\Exceptions\MissingSourceException;
use Deegitalbe\TrustupIoNotificationsContracts\Data\ToolsTestNotificationData;
use Deegitalbe\TrustupIoNotificationsContracts\Enums\Source;
use Deegitalbe\TrustupIoNotificationsContracts\Request\Recipient;
use Junges\Kafka\Facades\Kafka;

// -------------------------------------------------------------------------
// AC1: BuildNotificationRequest uses the recipient source when set
// -------------------------------------------------------------------------

it('uses the recipient source when the recipient already has a source', function (): void {
    Kafka::fake();

    config(['trustup-io-notifications.source' => null]);

    $recipient = Recipient::identified('user-1', Source::Tools);
    $data = new ToolsTestNotificationData('Title', 'Body');

    $envelope = app(BuildNotificationRequest::class)->execute($data, $recipient, null);

    /** @var array<string, mixed> $payload */
    $payload = $envelope->payload->toArray();

    expect($payload['recipient']['source'])->toBe(Source::Tools->value);
});

// -------------------------------------------------------------------------
// AC1: BuildNotificationRequest falls back to config source when recipient has none
// -------------------------------------------------------------------------

it('falls back to config source when anonymous recipient has no source', function (): void {
    Kafka::fake();

    config(['trustup-io-notifications.source' => Source::Tools->value]);

    $recipient = Recipient::anonymous('anon@example.com', null, [], locale: 'fr-BE');
    $data = new ToolsTestNotificationData('Title', 'Body');

    $envelope = app(BuildNotificationRequest::class)->execute($data, $recipient, null);

    /** @var array<string, mixed> $payload */
    $payload = $envelope->payload->toArray();

    expect($payload['recipient']['source'])->toBe(Source::Tools->value);
});

it('falls back to config source when an identified recipient has no source', function (): void {
    Kafka::fake();

    config(['trustup-io-notifications.source' => Source::Tools->value]);

    $recipient = Recipient::identified('user-1');
    $data = new ToolsTestNotificationData('Title', 'Body');

    $envelope = app(BuildNotificationRequest::class)->execute($data, $recipient, null);

    /** @var array<string, mixed> $payload */
    $payload = $envelope->payload->toArray();

    expect($payload['recipient']['source'])->toBe(Source::Tools->value)
        ->and($payload['recipient']['external_user_id'])->toBe('user-1')
        ->and($payload['recipient']['identified'])->toBeTrue();
});

// -------------------------------------------------------------------------
// AC1: BuildNotificationRequest throws MissingSourceException when no source resolves
// -------------------------------------------------------------------------

it('throws MissingSourceException when neither recipient source nor config source is set', function (): void {
    Kafka::fake();

    config(['trustup-io-notifications.source' => null]);

    $recipient = Recipient::anonymous('anon@example.com', null, [], locale: 'fr-BE');
    $data = new ToolsTestNotificationData('Title', 'Body');

    expect(fn () => app(BuildNotificationRequest::class)->execute($data, $recipient, null))
        ->toThrow(MissingSourceException::class);

    Kafka::assertNothingPublished();
});
