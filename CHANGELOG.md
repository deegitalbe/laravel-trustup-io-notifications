# @deegitalbe/laravel-trustup-io-notifications

## 1.0.4

### Patch Changes

- Updated dependencies [2a16b5b]
  - @deegitalbe/laravel-trustup-io-notifications-contracts@3.3.0

## 1.0.3

### Patch Changes

- f4562d2: Publish and consume through the contracts KafkaFactory

  - Replace the bare `Kafka::` producer and consumer calls with the injected contracts `KafkaFactory`, so publishing a request and consuming statuses/engagements authenticate against SASL brokers (Azure Event Hubs).
  - Read the Kafka connection and topics from the contracts-owned config; the client keeps its own `trustup-io-notifications` config for the `source` it stamps on outgoing events (the producer's identity, read only by the client).

- Updated dependencies [f4562d2]
  - @deegitalbe/laravel-trustup-io-notifications-contracts@3.2.0

## 1.0.2

### Patch Changes

- Updated dependencies [c7e1031]
  - @deegitalbe/laravel-trustup-io-notifications-contracts@3.1.0

## 1.0.1

### Patch Changes

- Updated dependencies [d74da23]
  - @deegitalbe/laravel-trustup-io-notifications-contracts@3.0.0

## 1.0.0

### Major Changes

- 970a5b8: Pass the notifiable to both `SendsTrustupIoNotification` methods, so a host notification can derive its payload and its channel restriction from the recipient instead of constructor state alone.

  - `toTrustupIoNotificationsData()` now takes a required `object $notifiable`.
  - `restrictTrustupIoNotificationsChannels()` now takes a required `object $notifiable`.
  - `TrustupIoNotificationsChannel::send()` forwards the notifiable it received to both calls, on the model path and on the `AnonymousNotifiable` path alike.
  - `InteractsWithTrustupIoNotifications` mirrors the parameter on its default restriction, which still returns `null`.
  - Breaking: every host notification implementing the contract must add the parameter to both methods. The parameter is typed `object` because the channel also accepts Laravel's `AnonymousNotifiable`.

## 0.5.12

### Patch Changes

- Updated dependencies [1f193ba]
  - @deegitalbe/laravel-trustup-io-notifications-contracts@2.0.0

## 0.5.11

### Patch Changes

- Updated dependencies [493c1b4]
  - @deegitalbe/laravel-trustup-io-notifications-contracts@1.0.0

## 0.5.10

### Patch Changes

- Updated dependencies [0d91807]
  - @deegitalbe/laravel-trustup-io-notifications-contracts@0.14.0

## 0.5.9

### Patch Changes

- Updated dependencies [9b248a6]
  - @deegitalbe/laravel-trustup-io-notifications-contracts@0.13.0

## 0.5.8

### Patch Changes

- Updated dependencies [c875cbf]
  - @deegitalbe/laravel-trustup-io-notifications-contracts@0.12.0

## 0.5.7

### Patch Changes

- Updated dependencies [75f994e]
  - @deegitalbe/laravel-trustup-io-notifications-contracts@0.11.0

## 0.5.6

### Patch Changes

- Updated dependencies [4a542d7]
  - @deegitalbe/laravel-trustup-io-notifications-contracts@0.10.0

## 0.5.5

### Patch Changes

- Updated dependencies [f6d5549]
  - @deegitalbe/laravel-trustup-io-notifications-contracts@0.9.0

## 0.5.4

### Patch Changes

- Updated dependencies [82b862b]
  - @deegitalbe/laravel-trustup-io-notifications-contracts@0.8.0

## 0.5.3

### Patch Changes

- Updated dependencies [90e41e4]
  - @deegitalbe/laravel-trustup-io-notifications-contracts@0.7.0

## 0.5.2

### Patch Changes

- Updated dependencies [20f2ed5]
- Updated dependencies [20f2ed5]
- Updated dependencies [20f2ed5]
  - @deegitalbe/laravel-trustup-io-notifications-contracts@0.6.0

## 0.5.1

### Patch Changes

- 59709ec: Align internal dependency constraint with monorepo-builder in CI
- Updated dependencies [59709ec]
  - @deegitalbe/laravel-trustup-io-notifications-contracts@0.5.1

## 0.5.0

### Minor Changes

- fd5ac83: Publish packages with the aligned dependency constraint

### Patch Changes

- Updated dependencies [fd5ac83]
  - @deegitalbe/laravel-trustup-io-notifications-contracts@0.5.0

## 0.4.0

### Minor Changes

- eaedf36: Publish packages to their Packagist mirrors

### Patch Changes

- Updated dependencies [eaedf36]
  - @deegitalbe/laravel-trustup-io-notifications-contracts@0.4.0

## 0.3.0

### Minor Changes

- b9787a5: Publish the Laravel client package to Packagist

### Patch Changes

- Updated dependencies [b9787a5]
  - @deegitalbe/laravel-trustup-io-notifications-contracts@0.3.0

## 0.2.0

### Minor Changes

- f27d163: First public release of the Laravel client package

  - Send notifications from a source application via `notify()`, with per-source routing and recipient resolution.
  - Consume delivery status and engagement feedback over Kafka.
  - Installable via `composer require deegitalbe/laravel-trustup-io-notifications`.

### Patch Changes

- Updated dependencies [f27d163]
  - @deegitalbe/laravel-trustup-io-notifications-contracts@0.2.0
