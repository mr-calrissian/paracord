import type { GuildChannel, GuildMember, Snowflake, User } from './types';
import Guild from './clients/Paracord/structures/Guild';
export declare function clone(object: Record<string, unknown>): Record<string, unknown>;
export declare function timestampNSecondsInFuture(seconds: number): number;
export declare function timestampNMillisecondsInFuture(milliseconds: number): number;
export declare function millisecondsFromNow(timestamp: number): number;
export declare function timestampFromSnowflake(snowflake: Snowflake): number;
export declare function coerceTokenToBotLike(token: string): string;
export declare function computeChannelPerms({ member, guild, channel, stopOnOwnerAdmin, }: {
    member: GuildMember;
    guild: Guild;
    channel: GuildChannel;
    stopOnOwnerAdmin?: boolean;
}): number;
export declare function computeGuildPerms({ member, guild, stopOnOwnerAdmin }: {
    member: GuildMember;
    guild: Guild;
    stopOnOwnerAdmin?: boolean;
}): number;
export declare function computeChannelOverwrites(perms: number, member: GuildMember, guild: Guild, channel: GuildChannel): number;
export declare function constructUserAvatarUrl(user: User, fileType?: string): string;
export declare function constructGuildIcon(guild: Guild, fileType?: string): string | undefined;
export declare function createUnsafeUuid(): string;
export declare function returnCreatedOn(resource: Record<'id', Snowflake>): number;
export declare function camelToSnake(str: string): string;
export declare function objectKeysCamelToSnake(obj: Record<string, unknown>): Record<string, unknown>;
export declare function snakeToCamel(str: string): string;
export declare function objectKeysSnakeToCamel(obj: Record<string, unknown>): Record<string, unknown>;
