# Event-member query deadline

Pass the remaining 45-second event-member child budget into the SQLite store.
Install it as a progress handler on both normal event-member and group-truth
writer connections. Map SQLite interrupt to a durable `deadline` defer receipt
and retain that cause through scheduler breadcrumbs and retry handling.
