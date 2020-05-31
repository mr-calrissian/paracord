
import { EventEmitter } from 'events';
import { DebugLevel, ILockServiceOptions, UserEvents } from '../../common';
import {
  LOG_LEVELS, LOG_SOURCES, MINUTE_IN_MILLISECONDS, SECOND_IN_MILLISECONDS,
} from '../../constants';
import { RemoteApiResponse } from '../../rpc/types';
import {
  EventFunction, EventFunctions, GuildMap, GuildMember, Identify, Message, PresenceMap, RawGuild, RawGuildMember, RawPresence, RawUser, ReadyEventFields, Snowflake, User, UserMap,
} from '../../types';
import {
  clone, coerceTokenToBotLike, objectKeysSnakeToCamel, timestampFromSnowflake,
} from '../../Utils';
import Api from '../Api/Api';
import { IApiOptions, IApiResponse } from '../Api/types';
import Gateway from '../Gateway/Gateway';
import { GatewayBotResponse, GatewayOptions } from '../Gateway/types';
import * as eventFuncs from './eventFuncs';
import Guild from './structures/Guild';
import { GatewayMap, ParacordLoginOptions, ParacordOptions } from './types';

const { PARACORD_SHARD_IDS, PARACORD_SHARD_COUNT } = process.env;

/* "Start up" refers to logging in to the gateway and waiting for all the guilds to be returned. By default, events will be suppressed during start up. */

/** A client that provides caching and limited helper functions. Integrates the Api and Gateway clients into a seamless experience. */
export default class Paracord extends EventEmitter {
  /** Discord bot token. */
  token: string;

  /** Whether or not the `init()` function has already been called. */
  initialized: boolean;

  /** User details given by Discord in the "Ready" event form the gateway. https://discordapp.com/developers/docs/topics/gateway#ready-ready-event-fields */
  user!: User;

  /** During a shard's start up, how many guilds may be unavailable before forcing ready. */
  unavailableGuildTolerance?: number;

  /** During a shard's start up, time in seconds to wait from the last GUILD_CREATE to force ready. */
  unavailableGuildWait?: number;

  /** Interval that will force shards as ready when within set thresholds. */
  startWithUnavailableGuildsInterval?: NodeJS.Timer

  /* Internal clients. */
  /** Client through which to make REST API calls to Discord. */
  api!: Api;

  /** Gateway clients keyed to their shard #. */
  gateways: GatewayMap;

  /** Gateways queue to log in. */
  gatewayLoginQueue: Gateway[];

  /** Identify lock service options passed to the gateway shards. */
  gatewayLockServiceOptions?: {
    mainServerOptions: ILockServiceOptions,
    serverOptions: ILockServiceOptions[],
  };

  apiOptions: Partial<IApiOptions>;

  gatewayOptions: Partial<GatewayOptions>;

  /* State that tracks the start up process. */
  /** Timestamp of the last gateway identify. */
  safeGatewayIdentifyTimestamp?: number;

  /** Gateways left to login on start up before emitting `PARACORD_STARTUP_COMPLETE` event. */
  gatewayWaitCount?: number;

  /** Shard currently in the initial phases of the gateway connection in progress. */
  startingGateway: Gateway | undefined;

  /** Guilds left to ingest on start up before emitting `PARACORD_STARTUP_COMPLETE` event. */
  guildWaitCount?: number;

  /** Timestamp of last GUILD_CREATE event on start up for the current `startingGateway`. */
  lastGuildTimestamp?: number;

  /* Client caches. */
  /** Guild cache. */
  guilds: GuildMap;

  /** User cache. */
  users: UserMap;

  presences: PresenceMap;

  /** Interval that coordinates gateway logins. */
  processGatewayQueueInterval?: NodeJS.Timer;

  /** Interval that removes objects from the presence and user caches. */
  sweepCachesInterval?:NodeJS.Timer;

  /** Interval that removes object from the redundant presence update cache. */
  sweepRecentPresenceUpdatesInterval?: NodeJS.Timer;

  /* User-defined event handling behavior. */
  /** Key:Value mapping DISCORD_EVENT to user's preferred emitted name for use when connecting to the gateway. */
  events?: UserEvents;

  /** During startup, if events should be emitted before `PARACORD_STARTUP_COMPLETE` is emitted. `GUILD_CREATE` events will never be emitted during start up. */
  allowEventsDuringStartup: boolean;

  preventLogin: boolean;

  request!: Api['request'];

  addRateLimitService!: Api['addRateLimitService'];

  addRequestService!: Api['addRequestService'];

  gatewayEvents: EventFunctions;

  /** Throws errors and warns if the parameters passed to the constructor aren't sufficient. */
  private static validateParams(token: string): void {
    if (token === undefined) {
      throw Error("client requires a 'token'");
    }
  }

  /**
   * Creates a new Paracord client.
   *
   * @param {string} token Discord bot token. Will be coerced into a bot token.
   * @param {ParacordOptions} options Settings for this Paracord instance.
   */
  public constructor(token: string, options: ParacordOptions = {}) {
    super();
    Paracord.validateParams(token);

    this.token = coerceTokenToBotLike(token);
    this.initialized = false;
    this.guilds = new Map();
    this.users = new Map();
    this.presences = new Map();
    this.gateways = new Map();
    this.gatewayLoginQueue = [];
    this.guildWaitCount = 0;
    this.allowEventsDuringStartup = false;
    this.preventLogin = false;
    this.apiOptions = {};
    this.gatewayOptions = {};

    Object.assign(this, options);

    if (options.autoInit !== false) {
      this.init();
    }
    this.bindTimerFunction();
    this.gatewayEvents = this.bindEventFunctions();
  }

  /** Gateway clients keyed to their shard #. */
  public get shards(): GatewayMap {
    return this.gateways;
  }

  /** @type Whether or not there are gateways currently starting up. */
  public get connecting(): boolean {
    return this.gatewayLoginQueue.length !== 0 || this.startingGateway !== undefined;
  }

  /*
   ********************************
   ********* CONSTRUCTOR **********
   ********************************
   */

  /** Binds `this` to the event functions defined in a separate file. */
  private bindEventFunctions(): EventFunctions {
    const funcs: EventFunctions = {};
    for (const prop of Object.getOwnPropertyNames(eventFuncs)) {
      if (typeof (<EventFunctions> eventFuncs)[prop] === 'function') {
        funcs[prop] = (<EventFunctions> eventFuncs)[prop].bind(this);
      }
    }

    return funcs;
  }

  /** Binds `this` to functions that are used in timeouts and intervals. */
  private bindTimerFunction(): void {
    this.sweepCaches = this.sweepCaches.bind(this);
    this.processGatewayQueue = this.processGatewayQueue.bind(this);
  }

  /*
   ********************************
   *********** INTERNAL ***********
   ********************************
   */

  /**
   * Processes a gateway event.
   * @param eventType The type of the event from the gateway. https://discordapp.com/developers/docs/topics/gateway#commands-and-events-gateway-events (Events tend to be emitted in all caps and underlines in place of spaces.)
   * @param data From Discord.
   * @param shard Shard id of the gateway that emitted this event.
   */
  public eventHandler(eventType: string, data: unknown, shard: number): unknown {
    let emit = data ?? typeof data === 'object' ? objectKeysSnakeToCamel(<Record<string, unknown>>data) : data;

    /** Method defined in ParacordEvents.js */
    const paracordEvent = <EventFunction | undefined> this.gatewayEvents[eventType];
    if (paracordEvent !== undefined) {
      emit = paracordEvent(data, shard);
    }

    if (this.startingGateway?.id === shard && this.guildWaitCount !== undefined) {
      if (eventType === 'GUILD_CREATE') {
        --this.guildWaitCount;
        this.checkIfDoneStarting();
        return undefined;
      }

      return this.allowEventsDuringStartup ? data : undefined;
    }

    return emit;
  }

  /**
   * Simple alias for logging events emitted by this client.
   * @param level Key of the logging level of this message.
   * @param message Content of the log.
   * @param data Data pertinent to the event.
   */
  public log(level: DebugLevel, message: string, data?: unknown): void {
    this.emit('DEBUG', {
      source: LOG_SOURCES.PARACORD,
      level: LOG_LEVELS[level],
      message,
      data,
    });
  }

  /**
   * Proxy emitter. Renames type with a key in `this.events`.
   * @param type Name of the event.
   * @param args Any arguments to send with the emitted event.
   */
  public emit(event: string, ...args: unknown[]): boolean {
    if (this.events === undefined || this.events[event] === undefined) {
      return super.emit(event, ...args);
    }

    return super.emit(this.events[event], ...args);
  }

  /*
   ********************************
   ************ LOGIN *************
   ********************************
   */

  /**
   * Connects to Discord's gateway and begins receiving and emitting events.
   * @param options Options used when logging in.
   */
  public async login(options: ParacordLoginOptions = {}): Promise<void> {
    const {
      unavailableGuildTolerance, unavailableGuildWait, allowEventsDuringStartup,
    } = options;

    if (!this.initialized) {
      this.init();
    }

    this.unavailableGuildTolerance = unavailableGuildTolerance;
    this.unavailableGuildWait = unavailableGuildWait;

    if (PARACORD_SHARD_IDS !== undefined) {
      options.shards = <[number, number]>PARACORD_SHARD_IDS.split(',').map((s) => Number(s));
      options.shardCount = Number(PARACORD_SHARD_COUNT);
      const message = `Injecting shard settings from shard launcher. Shard Ids: ${options.shards}. Shard count: ${options.shardCount}`;
      this.log('INFO', message);
    }

    this.startGatewayLoginInterval();
    await this.enqueueGateways(options);

    this.allowEventsDuringStartup = allowEventsDuringStartup || false;

    this.startSweepInterval();
  }

  /** Begins the interval that kicks off gateway logins from the queue. */
  private startGatewayLoginInterval(): void {
    this.processGatewayQueueInterval = setInterval(
      this.processGatewayQueue, SECOND_IN_MILLISECONDS,
    );
  }

  /** Takes a gateway off of the queue and logs it in. */
  async processGatewayQueue(): Promise<void> {
    const {
      preventLogin, gatewayLoginQueue, startingGateway, safeGatewayIdentifyTimestamp, unavailableGuildTolerance, unavailableGuildWait,
    } = this;

    if (
      !preventLogin
        && gatewayLoginQueue.length
        && startingGateway === undefined
        && safeGatewayIdentifyTimestamp !== undefined
        && new Date().getTime() > safeGatewayIdentifyTimestamp
    ) {
      const gateway = <Gateway> this.gatewayLoginQueue.shift();
      this.safeGatewayIdentifyTimestamp = 10 * SECOND_IN_MILLISECONDS; // arbitrary buffer

      this.startingGateway = gateway;
      try {
        await gateway.login();

        if (unavailableGuildTolerance && unavailableGuildWait) {
          this.startWithUnavailableGuildsInterval = setInterval(this.startWithUnavailableGuilds.bind(this, gateway), 1e3);
        }
      } catch (err) {
        this.log('FATAL', err.message, gateway);
        this.clearStartingShardState();
        gatewayLoginQueue.unshift(gateway);
      }
    }
  }

  private startWithUnavailableGuilds(gateway: Gateway): void {
    const {
      unavailableGuildTolerance, guildWaitCount, unavailableGuildWait, lastGuildTimestamp,
    } = this;

    const withinTolerance = guildWaitCount !== undefined && guildWaitCount <= <number>unavailableGuildTolerance;
    const timedOut = lastGuildTimestamp !== undefined && lastGuildTimestamp + <number>unavailableGuildWait * 1e3 < new Date().getTime();

    if (this.startingGateway === gateway && withinTolerance && timedOut) {
      const message = `Forcing startup complete for shard ${this.startingGateway.id} with ${this.guildWaitCount} unavailable guilds.`;
      this.log('WARNING', message);
      this.checkIfDoneStarting(true);
    }
  }

  /** Decides shards to spawn and pushes a gateway onto the queue for each one.
   * @param options Options used when logging in.
   */
  private async enqueueGateways(options: ParacordLoginOptions): Promise<void> {
    const { identity } = options;
    const { shards, shardCount } = options;
    if (shards !== undefined && shardCount !== undefined) {
      shards.forEach((s) => {
        if (s + 1 > <number> shardCount) {
          throw Error(`shard id ${s} exceeds max shard id of ${<number> shardCount - 1}`);
        }
      });
    }

    const { shards: spawnShards, shardCount: spawnCount } = await this.computeShards(shards, shardCount);

    if (spawnShards === undefined || spawnCount === undefined) {
      throw Error(`shards ids or shard count are invalid - ids ${spawnShards} , count: ${spawnCount}`);
    } else {
      spawnShards.forEach((shard) => {
        const identityCopy = clone(identity || {});
        identityCopy.shard = [shard, spawnCount];
        this.addNewGateway(identityCopy);
      });
    }
  }

  /**
   * Determines which shards will be spawned.
   * @param shards Shard Ids to spawn.
   * @param shardCount Total number of shards
   */
  private async computeShards(
    shards: number[] | undefined, shardCount: number | undefined,
  ): Promise<{shards: number[] | undefined, shardCount: number | undefined}> {
    if (shards !== undefined && shardCount === undefined) {
      throw Error('shards defined with no shardCount.');
    }

    if (shardCount === undefined) {
      const { status, data: { shards: recommendedShards } } = <GatewayBotResponse> await this.api.request(
        'get',
        'gateway/bot',
      );
      if (status === 200) {
        shardCount = recommendedShards;
      }
    }

    if (shards === undefined && shardCount !== undefined) {
      shards = [];
      for (let i = 0; i < shardCount; ++i) {
        shards.push(i);
      }
    }

    return { shards, shardCount };
  }

  /**
   * Creates gateway and pushes it into cache and login queue.
   * @param identity An object containing information for identifying with the gateway. https://discordapp.com/developers/docs/topics/gateway#identify-identify-structure
   */
  private addNewGateway(identity: Partial<Identify>): void {
    const gatewayOptions = {
      identity, api: this.api, emitter: this, events: this.events,
    };
    const gateway = this.setUpGateway(this.token, gatewayOptions);
    if (this.gateways.get(gateway.id) !== undefined) {
      throw Error(`duplicate shard id ${gateway.id}. shard ids must be unique`);
    }

    this.gatewayWaitCount && ++this.gatewayWaitCount;
    this.gateways.set(gateway.id, gateway);
    this.gatewayLoginQueue.push(gateway);
  }

  /** Sets up the internal handlers for this client. */
  public init(): void {
    if (this.initialized) {
      throw Error('Client has already been initialized.');
    }
    this.api = this.setUpApi(this.token, this.apiOptions);
    this.selfAssignHandlerFunctions();
    this.initialized = true;
  }


  /** Begins the intervals that prune caches. */
  public startSweepInterval(): void {
    this.sweepCachesInterval = setInterval(
      this.sweepCaches,
      60 * MINUTE_IN_MILLISECONDS,
    );
  }

  /*
   ********************************
   ************ SETUP *************
   ********************************
   */

  /**
   * Creates the handler used when handling REST calls to Discord.
   * @param token Discord token. Will be coerced to bot token.
   * @param options
   */
  private setUpApi(token: string, options: Partial<IApiOptions>): Api {
    const api = new Api(token, { ...options, emitter: this });
    if (api.rpcRequestService === undefined) {
      api.startQueue();
    }

    return api;
  }

  /**
   * Creates the handler used when connecting to Discord's gateway.
   * @param token Discord token. Will be coerced to bot token.
   * @param options
   */
  private setUpGateway(token: string, options: Partial<GatewayOptions>): Gateway {
    const gateway = new Gateway(token, {
      ...options,
      emitter: this,
      api: this.api,
    });

    if (this.gatewayLockServiceOptions) {
      const { mainServerOptions, serverOptions } = this.gatewayLockServiceOptions;
      gateway.addIdentifyLockServices(mainServerOptions, ...serverOptions);
    }

    return gateway;
  }

  /** Assigns some public functions from handlers to this client for easier access. */
  private selfAssignHandlerFunctions(): void {
    this.request = this.api.request.bind(this.api);
    this.addRateLimitService = this.api.addRateLimitService.bind(this.api);
    this.addRequestService = this.api.addRequestService.bind(this.api);
  }

  /**
   * Stores options that will be passed to each gateway shard when adding the service that will acquire a lock from a server(s) before identifying.
   * @param mainServerOptions Options for connecting this service to the identify lock server. Will not be released except by time out. Best used for global minimum wait time. Pass `null` to ignore.
   * @param serverOptions Options for connecting this service to the identify lock server. Will be acquired and released in order.
   */
  public addIdentifyLockServices(mainServerOptions: ILockServiceOptions, ...serverOptions: ILockServiceOptions[]): void {
    this.gatewayLockServiceOptions = {
      mainServerOptions,
      serverOptions,
    };
  }

  /*
   ********************************
   ********** START UP ************
   ********************************
   */

  /**
   * Prepares the client for caching guilds on start up.
   * @param data From Discord - Initial ready event after identify.
   */
  public handleReady(data: ReadyEventFields, shard: number): void {
    const { user, guilds } = data;

    this.guildWaitCount = guilds.length;
    guilds.forEach((g) => this.guilds.set(g.id, new Guild(g, this, shard)));

    user.tag = () => `${user.username}#${user.discriminator}`;
    this.user = user;
    this.log('INFO', `Logged in as ${user.tag}.`);

    const message = `Ready event received. Waiting on ${guilds.length} guilds.`;
    this.log('INFO', message);

    if (guilds.length === 0) {
      this.checkIfDoneStarting();
    } else {
      this.lastGuildTimestamp = new Date().getTime();
    }
  }

  /**
   * Runs with every GUILD_CREATE on initial start up. Decrements counter and emits `PARACORD_STARTUP_COMPLETE` when 0.
   * @param emptyShard Whether or not the shard started with no guilds.
   */
  private checkIfDoneStarting(forced?: boolean): void {
    const { startingGateway, guildWaitCount, gatewayWaitCount } = this;

    if (startingGateway !== undefined && gatewayWaitCount !== undefined) {
      if (forced || guildWaitCount === 0) {
        this.completeShardStartup(startingGateway, forced);

        this.gatewayWaitCount !== undefined && --this.gatewayWaitCount === 0 && this.completeStartup();
      } else if (guildWaitCount !== undefined && guildWaitCount < 0) {
        const message = `Shard ${startingGateway.id} - guildWaitCount is less than 0. This should not happen. guildWaitCount value: ${this.guildWaitCount}`;
        this.log('WARNING', message);
      } else {
        this.lastGuildTimestamp = new Date().getTime();
        const message = `Shard ${startingGateway.id} - ${guildWaitCount} guilds left in start up.`;
        this.log('INFO', message);
      }
    } else {
      const message = 'Starting check conducted without a defined gateway.';
      this.log('WARNING', message);
    }
  }

  private completeShardStartup(startingGateway: Gateway, forced = false): void {
    if (!forced) {
      const message = `Shard ${startingGateway.id} - received all start up guilds.`;
      this.log('INFO', message);
    }

    startingGateway.releaseIdentifyLocks();
    const shard = this.startingGateway;

    this.clearStartingShardState();

    this.emit('SHARD_STARTUP_COMPLETE', { shard, forced });
  }

  public clearStartingShardState(): void {
    this.startingGateway = undefined;
    this.lastGuildTimestamp = undefined;
    this.guildWaitCount = undefined;
    this.startWithUnavailableGuildsInterval && clearInterval(this.startWithUnavailableGuildsInterval);
  }

  /**
   * Cleans up Paracord start up process and emits `PARACORD_STARTUP_COMPLETE`.
   * @param reason Reason for the time out.
   */
  private completeStartup(reason?: string): void {
    let message = 'Paracord start up complete.';
    if (reason !== undefined) {
      message += ` ${reason}`;
    }

    this.log('INFO', message);
    this.emit('PARACORD_STARTUP_COMPLETE');
  }

  /*
   ********************************
   *********** CACHING ************
   ********************************
   */

  /**
   * Inserts/updates properties of a guild.
   * @param data From Discord - https://discordapp.com/developers/docs/resources/guild#guild-object
   * @param shard Id of shard that spawned this guild.
   * @param Guild Ignore. For dependency injection.
   */
  public upsertGuild(data: RawGuild, shard?: number, GuildConstructor = Guild): Guild | undefined {
    const cachedGuild = this.guilds.get(data.id);
    if (cachedGuild !== undefined) {
      return cachedGuild.mergeGuildData(data, this);
    }

    if (shard !== undefined) {
      const guild = new GuildConstructor(data, this, shard);
      this.guilds.set(data.id, guild);
      return guild;
    }

    return undefined;
  }

  /**
   * Inserts/updates user in this client's cache.
   * @param user From Discord - https://discordapp.com/developers/docs/resources/user#user-object-user-structure
   */
  public upsertUser(user: User | RawUser): User {
    let cachedUser = this.users.get(user.id) || <User>{};
    cachedUser.tag = () => `${user.username}#${user.discriminator}`;
    cachedUser = Object.assign(cachedUser, user);

    this.users.set(cachedUser.id, cachedUser);

    cachedUser.createdOn = () => timestampFromSnowflake(cachedUser.id);

    this.circularAssignCachedPresence(cachedUser);

    return cachedUser;
  }

  /**
   * Adjusts the client's presence cache, allowing ignoring events that may be redundant.
   * @param presence From Discord - https://discordapp.com/developers/docs/topics/gateway#presence-update
   */
  public updatePresences(presence: RawPresence): RawPresence {
    if (presence.status !== 'offline') {
      presence = this.upsertPresence(presence);
    } else {
      this.deletePresence(presence.user.id);
    }

    return presence;
  }

  /**
   * Inserts/updates presence in this client's cache.
   * @param presence From Discord - https://discordapp.com/developers/docs/topics/gateway#presence-update
   */
  private upsertPresence(presence: RawPresence): RawPresence {
    const cachedPresence = this.presences.get(presence.user.id);
    if (cachedPresence !== undefined) {
      presence = Object.assign(cachedPresence, presence);
    } else {
      this.presences.set(presence.user.id, presence);
    }

    this.circularAssignCachedUser(presence);

    return presence;
  }

  /**
   * Ensures that a user is assigned its presence from the cache and vice versa.
   * @param user From Discord - https://discordapp.com/developers/docs/resources/user#user-object
   */
  private circularAssignCachedPresence(user: User): void {
    const cachedPresence = this.presences.get(user.id);
    if (cachedPresence !== undefined) {
      user.presence = cachedPresence;
      user.presence.user = user;
    }
  }

  /**
   * Ensures that a presence is assigned its user from the cache and vice versa.
   * @param presence From Discord - https://discordapp.com/developers/docs/topics/gateway#presence-update
   */
  private circularAssignCachedUser(presence: RawPresence): void {
    let cachedUser;
    if (Object.keys(presence.user).length === 1) { // don't upsert if id is the only property
      cachedUser = this.users.get(presence.user.id);
    } else {
      cachedUser = this.upsertUser(presence.user);
    }

    if (cachedUser !== undefined) {
      presence.user = cachedUser;
      cachedUser.presence = presence;
    }
  }

  /**
   * Removes presence from cache.
   * @param userId Id of the presence's user.
   */
  private deletePresence(userId: Snowflake): void {
    this.presences.delete(userId);
    const user = this.users.get(userId);
    if (user !== undefined) {
      user.presence = undefined;
    }
  }

  /**
   * Processes presences (e.g. from PRESENCE_UPDATE, GUILD_MEMBERS_CHUNK, etc.)
   * @param guild Paracord guild.
   * @param presence From Discord. More information on a particular payload can be found in the official docs. https://discordapp.com/developers/docs/topics/gateway#presence-update
   */
  public handlePresence({ guild, presence }: { guild?: Guild; presence: RawPresence; }): RawPresence {
    const cachedPresence = this.updatePresences(presence);

    if (cachedPresence !== undefined) {
      guild && guild.setPresence(cachedPresence);
    } else {
      guild && guild.deletePresence(presence.user.id);
    }

    return cachedPresence;
  }

  /**
   * Processes a member object (e.g. from MESSAGE_CREATE, VOICE_STATE_UPDATE, etc.)
   * @param guild Paracord guild.
   * @param member From Discord. More information on a particular payload can be found in the official docs. https://discordapp.com/developers/docs/resources/guild#guild-member-object
   */
  public cacheMemberFromEvent(guild: Guild, member: GuildMember | RawGuildMember): GuildMember {
    const cachedMember = guild.members.get(member.user.id);
    return cachedMember === undefined ? guild.upsertMember(member, this) : cachedMember;
  }

  /** Removes from presence and user caches users who are no longer in a cached guild. */
  private sweepCaches(): void {
    const idsFromPresences = this.presences.keys();
    const idsFromUsers = this.users.keys();
    const deleteIds = Paracord.deDupe(Array.from(idsFromPresences).concat(Array.from(idsFromUsers)));

    Paracord.trimMembersFromDeleteList(deleteIds, this.guilds.values());

    let sweptCount = 0;
    for (const id of deleteIds.keys()) {
      this.clearUserFromCaches(id);
      ++sweptCount;
    }

    this.log('INFO', `Swept ${sweptCount} users from caches.`);
  }

  static uqSnowflakes(...args: (Map<string, unknown> | string)[]): string[] {
    let arr: string[] = [];
    for (const arg of args) {
      if (typeof arg === 'string') {
        arr = arr.concat(arg);
      } else {
        arr = arr.concat(Array.from(arg.keys()));
      }
    }

    return arr;
  }

  /** https://stackoverflow.com/questions/6940103/how-do-i-make-an-array-with-unique-elements-i-e-remove-duplicates */
  static deDupe(a: Array<Snowflake>): Map<Snowflake, undefined> {
    const temp: Map<Snowflake, undefined> = new Map();
    for (let i = 0; i < a.length; i++) {
      temp.set(a[i], undefined);
    }

    return temp;
  }

  /**
   * Remove users referenced in a guild's members or presences from the delete list.
   * @param deleteIds Unique set of user ids in a map.
   * @param guilds An iterable of guilds.
   *  */
  private static trimMembersFromDeleteList(deleteIds: Map<Snowflake, undefined>, guilds: IterableIterator<Guild>): void {
    for (const { members, presences } of guilds) {
      const idsFromPresences = presences.keys();
      const idFromMembers = members.keys();
      const cachedIds = Paracord.deDupe(Array.from(idsFromPresences).concat(Array.from(idFromMembers)));
      for (const id of cachedIds.keys()) {
        deleteIds.delete(id);
      }
    }
  }

  /**
   * Delete the user and its presence from this client's cache.
   * @param id User id.
   */
  private clearUserFromCaches(id: Snowflake) {
    this.presences.delete(id);
    this.users.delete(id);
  }


  /*
   ********************************
   ******* PUBLIC HELPERS *********
   ********************************
   */

  /**
   * Short-hand for sending a message to Discord.
   * @param channelId Discord snowflake of the channel to send the message.
   * @param message  When a string is passed for `message`, that string will populate the `content` field. https://discordapp.com/developers/docs/resources/channel#create-message-params
   */
  public sendMessage(channelId: Snowflake, message: string | Record<string, unknown> | Message): Promise<IApiResponse | RemoteApiResponse> {
    return this.request('post', `channels/${channelId}/messages`, {
      data:
        typeof message === 'string' ? { content: message } : { embed: message },
    });
  }

  /**
   * Short-hand for editing a message to Discord.
   *
   * @param message Partial Discord message. https://discordapp.com/developers/docs/resources/channel#create-message-params
   * @param newMessage  When a string is passed for `message`, that string will populate the `content` field. https://discordapp.com/developers/docs/resources/channel#create-message-params
   */
  public editMessage(message: Record<string, unknown> | Message, newMessage: string | Record<string, unknown> | Message): Promise<IApiResponse | RemoteApiResponse> {
    return this.request(
      'patch',
      `channels/${message.channelId}/messages/${message.id}`,
      {
        data:
        typeof newMessage === 'string'
          ? { content: newMessage }
          : { embed: newMessage },
      },
    );
  }

  /**
   * Fetch a guild member using the REST API, caching on successful hit.
   *
   * @param guild Guild object or id in which to search for member.
   * @param memberId Id of the member.
   */
  public async fetchMember(guild: Snowflake | Guild, memberId: Snowflake): Promise<IApiResponse | RemoteApiResponse> {
    let guildId;

    if (typeof guild !== 'string') {
      ({ id: guildId } = guild);
    }

    const res = await this.request('get', `/guilds/${guildId}/members/${memberId}`);

    if (res.status === 200) {
      let cacheGuild;
      if (typeof guild === 'string') {
        cacheGuild = this.guilds.get(guild);
      }

      if (cacheGuild !== undefined) {
        res.data = cacheGuild.upsertMember(<GuildMember>res.data, this);
      }
    }

    return res;
  }


  /**
   * Fetch a user using the REST API, caching on successful hit.
   * @param userId Id of the user.
   */
  public async fetchUser(userId: Snowflake): Promise<IApiResponse | RemoteApiResponse> {
    const res = await this.request('get', `/users/${userId}`);

    if (res.status === 200) {
      res.data = this.upsertUser(<User>res.data);
    }

    return res;
  }
}
