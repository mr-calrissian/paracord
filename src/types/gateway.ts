import {
  Snowflake, RawUser, ISO8601timestamp, RawRole, RawGuildMember, RawEmoji, UnavailableGuild,
} from '.';

export type GatewayPayload = {
  /** opcode for the payload */
  op: number;
  /** event data */
  d: string | number | boolean | null; // any JSON value;
  /** sequence number, used for resuming sessions and heartbeats */
  s: number | null; // *
  /** the event name for this payload */
  t: string | null; // *
};

// ========================================================================

export type GatewayURLParams = {
  /** Gateway Version to use */
  v: number; // see [Gateway versions](#DOCS_TOPICS_GATEWAY/gateways-gateway-versions)
  /** The encoding of received gateway packets */
  encoding: string; // 'json' or 'etf'
  /** The (optional) compression of gateway packets */
  compress?: string; // 'zlib-stream'
};

// ========================================================================

export type Identify = {
  /** authentication token */
  token: string;
  /** information about the client and how it's connecting */
  properties: IdentifyConnectionProperties;
  /** whether this connection supports compression of packets */
  compress?: boolean; // false
  /** value between 50 and 250, total number of members where the gateway will stop sending offline members in the guild member list */
  large_threshold?: number; // 50
  /** used for Guild Sharding */
  shard?: [number, number]; // (shard_id, num_shards);
  /** presence structure for initial presence information */
  presence?: GatewayStatusUpdate;
  /** enables dispatching of guild subscription events (presence and typing events) */
  guild_subscriptions?: boolean; // true
  /** the Gateway Intents you wish to receive */
  intents?: number;
};

// ========================================================================

export type IdentifyConnectionProperties = {
  /** your operating system */
  $os: string;
  /** your library name */
  $browser: string;
  /** your library name */
  $device: string;
};

// ========================================================================

export type Resume = {
  /** session token */
  token: string;
  /** session id */
  session_id: string;
  /** last sequence number received */
  seq: null | number;
};

// ========================================================================

export type GuildRequestMembers = {
  /** id of the guild(s) to get members for */
  guild_id: Snowflake | Snowflake[]; // true
  /** string that username starts with, or an empty string to return all members */
  query?: string; // one of query or user_ids
  /** maximum number of members to send matching the `query`; a limit of `0` can be used with an empty string `query` to return all members */
  limit: number; // true when specifying query
  /** used to specify if we want the presences of the matched members */
  presences?: boolean; // false
  /** used to specify which users you wish to fetch */
  user_ids?: Snowflake | Snowflake[]; // one of query or user_ids
  /** nonce to identify the Guild Members Chunk response */
  nonce?: string; // false
};

// ========================================================================

export type GatewayVoiceStateUpdate = {
  /** id of the guild */
  guild_id: Snowflake;
  /** id of the voice channel client wants to join (null if disconnecting) */
  channel_id: Snowflake | null;
  /** is the client muted */
  self_mute: boolean;
  /** is the client deafened */
  self_deaf: boolean;
};

// ========================================================================

export type GatewayStatusUpdate = {
  /** unix time (in milliseconds) of when the client went idle, or null if the client is not idle */
  since: number | null;
  /** null, or the user's activities */
  activities: RawActivity[] | null;
  /** the user's new status */
  status: string;
  /** whether or not the client is afk */
  afk: boolean;
};

// ========================================================================

export type StatusTypes = [
  /** Online */
  'online' |
  /** Do Not Disturb */
  'dnd' |
  /** AFK */
  'idle' |
  /** Invisible and shown as offline */
  'invisible' |
  /** Offline */
  'offline'
];

// ========================================================================

export type Hello = {
  /** the interval (in milliseconds) the client should heartbeat with */
  heartbeat_interval: number;
};

// ========================================================================

export type ReadyEventFields = {
  /** gateway version */
  v: number;
  /** information about the user including email */
  user: RawUser;
  /** empty array */
  private_channels: [];
  /** the guilds the user is in */
  guilds: UnavailableGuild[];
  /** used for resuming connections */
  session_id: string;
  /** the shard information associated with this session, if sent when identifying */
  shard?: [number, number]; // (shard_id, num_shards);
};

// ========================================================================

export type ChannelPinsUpdateEventFields = {
  /** the id of the guild */
  guild_id?: Snowflake;
  /** the id of the channel */
  channel_id: Snowflake;
  /** the time at which the most recent pinned message was pinned */
  last_pin_timestamp?: ISO8601timestamp;
};

// ========================================================================

export type GuildBanAddEventFields = {
  /** id of the guild */
  guild_id: Snowflake;
  /** the banned user */
  user: RawUser;
};

// ========================================================================

export type GuildBanRemoveEventFields = {
  /** id of the guild */
  guild_id: Snowflake;
  /** the unbanned user */
  user: RawUser;
};

// ========================================================================

export type GuildEmojisUpdateEventFields = {
  /** id of the guild */
  guild_id: Snowflake;
  /** array of emojis */
  emojis: [];
};

// ========================================================================

export type GuildIntegrationsUpdateEventFields = {
  /** id of the guild whose integrations were updated */
  guild_id: Snowflake;
};

// ========================================================================

export type GuildMemberAddExtraFields = {
  /** id of the guild */
  guild_id: Snowflake;
};

// ========================================================================

export type GuildMemberRemoveEventFields = {
  /** the id of the guild */
  guild_id: Snowflake;
  /** the user who was removed */
  user: RawUser;
};

// ========================================================================

export type GuildMemberUpdateEventFields = {
  /** the id of the guild */
  guild_id: Snowflake;
  /** user role ids */
  roles: Snowflake[];
  /** the user */
  user: RawUser;
  /** nickname of the user in the guild */
  nick?: string | null;
  /** when the user joined the guild */
  joined_at: ISO8601timestamp;
  /** when the user starting boosting the guild */
  premium_since?: ISO8601timestamp | null;
  /** whether the user is deafened in voice channels */
  deaf: boolean | undefined;
  /** whether the user is muted in voice channels */
  mute: boolean | undefined;
};

// ========================================================================

export type GuildMembersChunkEventFields = {
  /** the id of the guild */
  guild_id: Snowflake;
  /** set of guild members */
  members: RawGuildMember[];
  /** the chunk index in the expected chunks for this response (0 <= chunk\_index < chunk\_count) */
  chunk_index: number;
  /** the total number of expected chunks for this response */
  chunk_count: number;
  /** if passing an invalid id to `REQUEST_GUILD_MEMBERS`, it will be returned here */
  not_found?: [];
  /** if passing true to `REQUEST_GUILD_MEMBERS`, presences of the returned members will be here */
  presences?: RawPresence[];
  /** the nonce used in the Guild Members Request */
  nonce?: string;
};

// ========================================================================

export type GuildRoleCreateEventFields = {
  /** the id of the guild */
  guild_id: Snowflake;
  /** the role created */
  role: RawRole;
};

// ========================================================================

export type GuildRoleUpdateEventFields = {
  /** the id of the guild */
  guild_id: Snowflake;
  /** the role updated */
  role: RawRole;
};

// ========================================================================

export type GuildRoleDeleteEventFields = {
  /** id of the guild */
  guild_id: Snowflake;
  /** id of the role */
  role_id: Snowflake;
};

// ========================================================================

export type InviteCreateEventFields = {
  /** the channel the invite is for */
  channel_id: Snowflake;
  /** the unique invite code */
  code: string;
  /** unix timestamp of the time at which the invite was created */
  created_at: number;
  /** the guild of the invite */
  guild_id?: Snowflake;
  /** the user that created the invite */
  inviter?: RawUser;
  /** how long the invite is valid for (in seconds) */
  max_age: number;
  /** the maximum number of times the invite can be used */
  max_uses: number;
  /** the target user for this invite */
  target_user?: Partial<RawUser>;
  /** the type of user target for this invite */
  target_user_type?: number;
  /** whether or not the invite is temporary (invited users will be kicked on disconnect unless they're assigned a role) */
  temporary: boolean;
  /** how many times the invite has been used (always will be 0) */
  uses: number;
};

// ========================================================================

export type InviteDeleteEventFields = {
  /** the channel of the invite */
  channel_id: Snowflake;
  /** the guild of the invite */
  guild_id?: Snowflake;
  /** the unique invite code */
  code: string;
};

// ========================================================================

export type MessageDeleteEventFields = {
  /** the id of the message */
  id: Snowflake;
  /** the id of the channel */
  channel_id: Snowflake;
  /** the id of the guild */
  guild_id?: Snowflake;
};

// ========================================================================

export type MessageDeleteBulkEventFields = {
  /** the ids of the messages */
  ids: Snowflake[];
  /** the id of the channel */
  channel_id: Snowflake;
  /** the id of the guild */
  guild_id?: Snowflake;
};

// ========================================================================

export type MessageReactionAddEventFields = {
  /** the id of the user */
  user_id: Snowflake;
  /** the id of the channel */
  channel_id: Snowflake;
  /** the id of the message */
  message_id: Snowflake;
  /** the id of the guild */
  guild_id?: Snowflake;
  /** the member who reacted if this happened in a guild */
  member?: RawGuildMember;
  /** the emoji used to react - example */
  emoji: Partial<RawEmoji>;
};

// ========================================================================

export type MessageReactionRemoveEventFields = {
  /** the id of the user */
  user_id: Snowflake;
  /** the id of the channel */
  channel_id: Snowflake;
  /** the id of the message */
  message_id: Snowflake;
  /** the id of the guild */
  guild_id?: Snowflake;
  /** the emoji used to react - example */
  emoji: Partial<RawEmoji>;
};

// ========================================================================

export type MessageReactionRemoveAllEventFields = {
  /** the id of the channel */
  channel_id: Snowflake;
  /** the id of the message */
  message_id: Snowflake;
  /** the id of the guild */
  guild_id?: Snowflake;
};

// ========================================================================

export type MessageReactionRemoveEmoji = {
  /** the id of the channel */
  channel_id: Snowflake;
  /** the id of the guild */
  guild_id?: Snowflake;
  /** the id of the message */
  message_id: Snowflake;
  /** the emoji that was removed */
  emoji: Partial<RawEmoji>;
};

// ========================================================================

export type RawPresence = {
  /** the user presence is being updated for */
  user: RawUser;
  /** id of the guild */
  guild_id: Snowflake;
  /** either "idle", "dnd", "online", or "offline" */
  status: 'idle' | 'dnd' | 'online' | 'offline';
  /** user's current activities */
  activities: RawActivity[];
  /** user's platform-dependent status */
  client_status: ClientStatus;
};

// ========================================================================

export type ClientStatus = {
  /** the user's status set for an active desktop (Windows, Linux, Mac) application session */
  desktop?: string;
  /** the user's status set for an active mobile (iOS, Android) application session */
  mobile?: string;
  /** the user's status set for an active web (browser, bot account) application session */
  web?: string;
};

// ========================================================================

export type RawActivity = {
  /** the activity's name */
  name: string;
  /** activity type */
  type: number;
  /** stream url, is validated when type is 1 */
  url?: string | null;
  /** unix timestamp of when the activity was added to the user's session */
  created_at?: number;
  /** unix timestamps for start and/or end of the game */
  timestamps?: ActivityTimestamps;
  /** application id for the game */
  application_id?: Snowflake;
  /** what the player is currently doing */
  details?: string | null;
  /** the user's current party status */
  state?: string | null;
  /** the emoji used for a custom status */
  emoji?: ActivityEmoji | null;
  /** information for the current party of the player */
  party?: ActivityParty;
  /** images for the presence and their hover texts */
  assets?: ActivityAssets;
  /** secrets for Rich Presence joining and spectating */
  secrets?: ActivitySecrets;
  /** whether or not the activity is an instanced game session */
  instance?: boolean;
  /** activity flags `OR`d together, describes what the payload includes */
  flags?: number;
};

// ========================================================================

export type ActivityTypes = [
  /** Game */
  0 |
  /** Streaming */
  1 |
  /** Listening */
  2 |
  /** Custom */
  4 |
  /** Competing */
  5
];

// ========================================================================

export type ActivityTimestamps = {
  /** unix time (in milliseconds) of when the activity started */
  start?: number;
  /** unix time (in milliseconds) of when the activity ends */
  end?: number;
};

// ========================================================================

export type ActivityEmoji = {
  /** the name of the emoji */
  name: string;
  /** the id of the emoji */
  id?: Snowflake;
  /** whether this emoji is animated */
  animated?: boolean;
};

// ========================================================================

export type ActivityParty = {
  /** the id of the party */
  id?: string;
  /** used to show the party's current and maximum size */
  size?: [number, number]; // ((currentSize, maxSize));
};

// ========================================================================

export type ActivityAssets = {
  /** the id for a large asset of the activity, usually a snowflake */
  large_image?: string;
  /** text displayed when hovering over the large image of the activity */
  large_text?: string;
  /** the id for a small asset of the activity, usually a snowflake */
  small_image?: string;
  /** text displayed when hovering over the small image of the activity */
  small_text?: string;
};

// ========================================================================

export type ActivitySecrets = {
  /** the secret for joining a party */
  join?: string;
  /** the secret for spectating a game */
  spectate?: string;
  /** the secret for a specific instanced match */
  match?: string;
};

// ========================================================================

export enum ActivityFlags {
  INSTANCE = 1 << 0,
  JOIN = 1 << 1,
  SPECTATE = 1 << 2,
  JOIN_REQUEST = 1 << 3,
  SYNC = 1 << 4,
  PLAY = 1 << 5
}

// ========================================================================

export type TypingStartEventFields = {
  /** id of the channel */
  channel_id: Snowflake;
  /** id of the guild */
  guild_id?: Snowflake;
  /** id of the user */
  user_id: Snowflake;
  /** unix time (in seconds) of when the user started typing */
  timestamp: number;
  /** the member who started typing if this happened in a guild */
  member?: RawGuildMember;
};

// ========================================================================

export type VoiceServerUpdateEventFields = {
  /** voice connection token */
  token: string;
  /** the guild this voice server update is for */
  guild_id: Snowflake;
  /** the voice server host */
  endpoint: string;
};

// ========================================================================

export type WebhookUpdateEventFields = {
  /** id of the guild */
  guild_id: Snowflake;
  /** id of the channel */
  channel_id: Snowflake;
};
