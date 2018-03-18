**Note: currently in development, not ready for production!** Only test in safe environments.

# gsredis

**gsredis** is a plugin for the [Glowstone](https://github.com/GlowstoneMC/Glowstone) server software,
that replaces the default [Anvil file format](https://minecraft.gamepedia.com/Anvil_file_format) used by
Glowstone and other PC-edition servers (e.g. Vanilla, Spigot, Sponge) with a [redis data store](https://redis.io/).

## Why would I need this?

All of the mentioned server software, including Glowstone, use the file system
to store and read world data (chunks). This makes it hard to have a centralized,
concurrent storage system for multiple servers.

For example, a minigame server network with 10+ servers running the same world will need
to deploy map changes on each server. Using network-based file storage solutions can
be tedious and slow, especially because of file locks and concurrency issues.

Using a centralized and concurrent data storage system like **redis**, you can efficiently
read and write world data to a common system. This also makes deployment a breeze,
as you only need to deploy changes to the central data store and restart server instances.
Each server will then stream world data directly from the redis server.

![Schema](https://i.imgur.com/TPkodZB.png)

## Why Glowstone?

Vanilla and wrappers around it (Spigot, Paper, Sponge) have their world I/O code strictly tied to
the file system (Anvil format). There is no easy way to adapt the system using only plugins on these platforms.

[Glowstone](https://github.com/GlowstoneMC/Glowstone) is a clean-room rewrite of the Vanilla
server software, and uses the Bukkit/Spigot/Paper plugin API. It is a more efficient server software,
but because it is a rewrite, there may be inconsistencies and missing features between it and Vanilla wrappers.

Around the end of 2017, we introduced [an API](https://glowstone.net/jd/glowstone/net/glowstone/io/WorldStorageProvider.html) that lets plugins developed for Glowstone
to use custom world storage solutions, while keeping the Anvil file format as the default.
*gsredis* uses that API to interface between the Redis client and Glowstone.

## Does it work?

This project is currently work-in-progress and certain essential aspects of chunk storage are currently broken.
Because of technical limitations, I do not plan to implement entity storage as part of this plugin.