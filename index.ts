import { createBuffer } from '@posthog/plugin-contrib'
import { Plugin, PluginMeta, PluginEvent, Properties } from '@posthog/plugin-scaffold'
import { Client } from 'pg'

type RedshiftPlugin = Plugin<{
    global: {
        pgClient: Client
        buffer: ReturnType<typeof createBuffer>
        eventsToIgnore: Set<string>
        sanitizedTableName: string
    }
    config: {
        clusterHost: string
        clusterPort: string
        dbName: string
        tableName: string
        dbUsername: string
        dbPassword: string
        uploadSeconds: string
        uploadMegabytes: string
        eventsToIgnore: string
        propertiesDataType: string
    }
}>

type RedshiftMeta = PluginMeta<RedshiftPlugin>

interface ParsedEvent {
    uuid: string
    eventName: string
    properties: string
    elements: string
    set: string
    set_once: string
    distinct_id: string
    team_id: number
    ip: string
    site_url: string
    timestamp: string

    shopKey: string
    session_id: string
    page: string
    continent_name: string
    country_name: string
    browser_language: string
    device_type: string
    referrer: string
}

type InsertQueryValue = string | number

interface UploadJobPayload {
    batch: ParsedEvent[]
    batchId: number
    retriesPerformedSoFar: number
}

export const jobs: RedshiftPlugin['jobs'] = {
    uploadBatchToRedshift: async (payload: UploadJobPayload, meta: RedshiftMeta) => {
        await insertBatchIntoRedshift(payload, meta)
    },
}

export const setupPlugin: RedshiftPlugin['setupPlugin'] = async (meta) => {
    const { global, config } = meta

    const requiredConfigOptions = ['clusterHost', 'clusterPort', 'dbName', 'dbUsername', 'dbPassword']
    for (const option of requiredConfigOptions) {
        if (!(option in config)) {
            throw new Error(`Required config option ${option} is missing!`)
        }
    }

    if (!config.clusterHost.endsWith('redshift.amazonaws.com')) {
        throw new Error('Cluster host must be a valid AWS Redshift host')
    }

    // Max Redshift insert is 16 MB: https://docs.aws.amazon.com/redshift/latest/dg/c_redshift-sql.html
    const uploadMegabytes = Math.max(1, Math.min(parseInt(config.uploadMegabytes) || 1, 10))
    const uploadSeconds = Math.max(1, Math.min(parseInt(config.uploadSeconds) || 1, 600))

    global.sanitizedTableName = sanitizeSqlIdentifier(config.tableName)

    const propertiesDataType = config.propertiesDataType === 'varchar' ? 'varchar(65535)' : 'super'

    const queryError = await executeQuery(
        `CREATE TABLE IF NOT EXISTS public.${global.sanitizedTableName} (
            uuid varchar(200),
            event varchar(200),
            properties ${propertiesDataType},
            elements varchar(65535),
            set ${propertiesDataType},
            set_once ${propertiesDataType},
            timestamp timestamp with time zone,
            team_id int,
            distinct_id varchar(200),
            ip varchar(200),
            site_url varchar(200),
            
            shopkey varchar(3900),
            session_id varchar(200),
            page varchar(200),
            continent_name varchar(200),
            country_name varchar(200),
            browser_language varchar(200),
            device_type varchar(200),
            referrer varchar(65535)
        );`,
        [],
        config,
    )

    if (queryError) {
        throw new Error(`Unable to connect to Redshift cluster and create table with error: ${queryError.message}`)
    }

    global.buffer = createBuffer({
        limit: uploadMegabytes * 1024 * 1024,
        timeoutSeconds: uploadSeconds,
        onFlush: async (batch) => {
            await insertBatchIntoRedshift(
                { batch, batchId: Math.floor(Math.random() * 1000000), retriesPerformedSoFar: 0 },
                meta,
            )
        },
    })

    global.eventsToIgnore = new Set(
        config.eventsToIgnore ? config.eventsToIgnore.split(',').map((event) => event.trim()) : null,
    )
}

export async function onEvent(event: PluginEvent, { global }: RedshiftMeta) {
    const {
        event: eventName,
        properties,
        $set,
        $set_once,
        distinct_id,
        team_id,
        site_url,
        now,
        sent_at,
        uuid,
        ..._discard
    } = event

    const ip = properties?.['$ip'] || event.ip
    const timestamp = event.timestamp || properties?.timestamp || now || sent_at
    let ingestedProperties = properties
    let elements = []

    // only move prop to elements for the $autocapture action
    if (eventName === '$autocapture' && properties && '$elements' in properties) {
        const { $elements, ...props } = properties
        ingestedProperties = props
        elements = $elements
    }

    sanitizeUrls(ingestedProperties)
    sanitizeUrls($set_once)
    sanitizeUrls($set)

    const parsedEvent = {
        uuid,
        eventName,
        properties: JSON.stringify(ingestedProperties || {}),
        elements: JSON.stringify(elements || {}),
        set: JSON.stringify($set || {}),
        set_once: JSON.stringify($set_once || {}),
        distinct_id,
        team_id,
        ip,
        site_url,
        timestamp: new Date(timestamp).toISOString(),

        shopKey: ingestedProperties?.shop_key || '',
        session_id: ingestedProperties?.$session_id || ingestedProperties?.checkout_session_id || '',
        page: ingestedProperties?.checkout_page || ingestedProperties?.payment_page || '',
        continent_name: ingestedProperties?.$geoip_continent_name || '',
        country_name: ingestedProperties?.$geoip_country_name || '',
        browser_language: ingestedProperties?.browser_language || '',
        device_type: ingestedProperties?.$device_type || '',
        referrer: ingestedProperties?.$referrer || '',
    }

    if (!global.eventsToIgnore.has(eventName)) {
        global.buffer.add(parsedEvent)
    }
}

export const insertBatchIntoRedshift = async (payload: UploadJobPayload, { global, jobs, config }: RedshiftMeta) => {
    let values: InsertQueryValue[] = []
    let valuesString = ''

    const isSuper = config.propertiesDataType === 'super'

    for (let i = 0; i < payload.batch.length; ++i) {
        const {
            uuid,
            eventName,
            properties,
            elements,
            set,
            set_once,
            distinct_id,
            team_id,
            ip,
            site_url,
            timestamp,

            shopKey,
            session_id,
            page,
            continent_name,
            country_name,
            browser_language,
            device_type,
            referrer,
        } =
            payload.batch[i]
        const payloadItemCount = 19;

        // if is varchar using parametrised query
        // Creates format: ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, ...)
        // if is super type using plain text query
        // assemble the value into valueString and values is not needed
        if (isSuper) {
            valuesString += ` ('${uuid}', '${eventName}', JSON_PARSE('${properties.replace(/'/g, '\'\'')}'), '${elements}', JSON_PARSE('${set.replace(/'/g, '\'\'')}'), JSON_PARSE('${set_once.replace(/'/g, '\'\'')}'), '${distinct_id}', ${team_id}, '${ip}', '${site_url}', '${timestamp}', '${shopKey}', '${session_id}', '${page}', '${continent_name}' '${country_name}' '${browser_language}' '${device_type}' '${referrer}') ${i === payload.batch.length - 1 ? '' : ','}`
        } else {
            valuesString += ' ('
            for (let j = 1; j <= payloadItemCount; ++j) {
                valuesString += `$${payloadItemCount * i + j}${j === payloadItemCount ? '' : ', '}`
            }
            valuesString += `)${i === payload.batch.length - 1 ? '' : ','}`

            values = [
                ...values,
                ...[uuid, eventName, properties, elements, set, set_once, distinct_id, team_id, ip, site_url, timestamp, shopKey, session_id, page, continent_name, country_name, browser_language, device_type, referrer],
            ]
        }
    }

    console.log(
        `(Batch Id: ${payload.batchId}) Flushing ${payload.batch.length} event${
            payload.batch.length > 1 ? 's' : ''
        } to RedShift`,
    )

    const queryError = await executeQuery(
        `INSERT INTO ${global.sanitizedTableName} (uuid, event, properties, elements, set, set_once, distinct_id, team_id, ip, site_url, timestamp, shopkey, session_id, page, continent_name, country_name, browser_language, device_type, referrer)
        VALUES ${valuesString}`,
        values,
        config,
    )

    if (queryError) {
        console.error(`(Batch Id: ${payload.batchId}) Error uploading to Redshift: ${queryError.message}`)
        if (payload.retriesPerformedSoFar >= 15) {
            return
        }
        const nextRetryMs = 2 ** payload.retriesPerformedSoFar * 3000
        console.log(`Enqueued batch ${payload.batchId} for retry in ${nextRetryMs}ms`)
        await jobs
            .uploadBatchToRedshift({
                ...payload,
                retriesPerformedSoFar: payload.retriesPerformedSoFar + 1,
            })
            .runIn(nextRetryMs, 'milliseconds')
    }
}

const executeQuery = async (query: string, values: any[], config: RedshiftMeta['config']): Promise<Error | null> => {
    const pgClient = new Client({
        user: config.dbUsername,
        password: config.dbPassword,
        host: config.clusterHost,
        database: config.dbName,
        port: parseInt(config.clusterPort),
    })

    let error: Error | null = null
    try {
        await pgClient.connect()
        await pgClient.query(query, values)
    } catch (err: any) {
        console.error(`Error executing query: ${err.message}`)
        console.error(`Query: ${query}`)
        console.error(`uuid: ${values[0]}`)
        error = err
    } finally {
        await pgClient.end()
    }

    return error
}

export const teardownPlugin: RedshiftPlugin['teardownPlugin'] = ({ global }) => {
    global.buffer.flush()
}

const sanitizeSqlIdentifier = (unquotedIdentifier: string): string => {
    return unquotedIdentifier.replace(/[^\w\d_.]+/g, '')
}

const sanitizeUrls = (properties: Properties | undefined): Properties | undefined => {
    if (properties) {
        if (properties.$current_url) {
            properties.$current_url = properties.$current_url.replace('\\', '')
        }
        if (properties.$referrer) {
            properties.$referrer = properties.$referrer.replace('\\', '')
        }
        if (properties.$initial_current_url) {
            properties.$initial_current_url = properties.$initial_current_url.replace('\\', '')
        }
        if (properties.$initial_referrer) {
            properties.$initial_referrer = properties.$initial_referrer.replace('\\', '')
        }
    }

    return properties
}
