import axios from "axios";

export const api = axios.create({
  baseURL: "/api",
  timeout: 15000,
});

export type Asset = {
  id: number;
  order_id: string;
  bundle_title: string;
  product_title: string;
  platform: string;
  category: string;
  file_name: string;
  url?: string;
  download_urls?: string[] | string;
  image_url?: string;
  description?: string;
  tags?: string;
  ext?: string;
  size_bytes?: number;
  downloaded?: number;
  exists?: boolean;
  activation_key?: string;
};

export type Status = {
  ready: boolean;
  stats: { total: number; downloaded: number };
  ai_configured: boolean;
  session_valid?: boolean;
};

export async function fetchStatus(): Promise<Status> {
  const { data } = await api.get("/status");
  return data;
}

export async function fetchAssets(params: Record<string, string | number | undefined> = {}) {
  const { data } = await api.get("/assets", { params });
  return data.items as Asset[];
}

export async function fetchAsset(id: string) {
  const { data } = await api.get(`/assets/${id}`);
  return data as Asset;
}

export async function fetchBundles() {
  const { data } = await api.get("/bundles");
  return data;
}

export async function fetchPurchases() {
  const { data } = await api.get("/purchases");
  return data;
}

export async function fetchFacets(downloadedOnly?: boolean) {
  const { data } = await api.get("/facets", { params: downloadedOnly ? { downloaded: 1 } : {} });
  return data as { categories: string[]; platforms: string[]; exts: string[]; bundles: string[] };
}

export type Settings = {
  session_cookie: string;
  library_path: string;
  include: string[];
  exclude: string[];
  platforms: string[];
  trove: boolean;
  openwebui_url: string;
  openwebui_model: string;
  openwebui_api_key: string;
  auth_header_name: string;
  auth_header_value: string;
};

export async function fetchSettings(): Promise<Settings> {
  const { data } = await api.get("/settings");
  return data;
}

export async function updateSettings(payload: Partial<Settings>) {
  const { data } = await api.post("/settings", payload);
  return data;
}
