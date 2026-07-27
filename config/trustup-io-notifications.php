<?php

declare(strict_types=1);

return [
    'topics' => [
        'request' => env('TRUSTUP_IO_NOTIFICATIONS_TOPIC_REQUEST', 'notifications.request'),
        'status' => env('TRUSTUP_IO_NOTIFICATIONS_TOPIC_STATUS', 'notifications.status'),
        'dlq' => env('TRUSTUP_IO_NOTIFICATIONS_TOPIC_DLQ', 'notifications.dlq'),
        'engagement' => env('TRUSTUP_IO_NOTIFICATIONS_TOPIC_ENGAGEMENT', 'notifications.engagement'),
    ],

    'source' => env('TRUSTUP_IO_NOTIFICATIONS_SOURCE'),

    'channel_key' => 'trustup-io-notifications',
];
