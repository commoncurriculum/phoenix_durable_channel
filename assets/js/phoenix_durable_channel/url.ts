export type Params = Record<string, unknown>

/** Phoenix's query string: nested objects become `parent[child]=value`. */
export function serialize(params: Params, parentKey?: string): string {
  return Object.entries(params)
    .map(([key, value]) => {
      const paramKey = parentKey ? `${parentKey}[${key}]` : key
      if (typeof value === "object" && value !== null)
        return serialize(Object.fromEntries(Object.entries(value)), paramKey)
      return `${encodeURIComponent(paramKey)}=${encodeURIComponent(String(value))}`
    })
    .join("&")
}

export function appendParams(url: string, params: Params): string {
  if (Object.keys(params).length === 0) return url
  const prefix = url.includes("?") ? "&" : "?"
  return `${url}${prefix}${serialize(params)}`
}
