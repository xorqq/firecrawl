import { configDotenv } from "dotenv";
import { config } from "../../../config";
import { parse } from "tldts";
import { TeamFlags } from "../../../controllers/v1/types";
import { supabase_rr_service } from "../../../services/supabase";

configDotenv();

type BlocklistBlob = {
  blocklist: string[];
  allowedKeywords: string[];
};

let blob: BlocklistBlob | null = null;

export async function initializeBlocklist() {
  if (config.USE_DB_AUTHENTICATION !== true || config.DISABLE_BLOCKLIST) {
    blob = {
      blocklist: [],
      allowedKeywords: [],
    };
    return;
  }

  const { data, error } = await supabase_rr_service
    .from("blocklist")
    .select("*")
    .single();

  if (error) {
    throw new Error(`Error getting blocklist: ${error.message}`);
  }

  if (!data) {
    throw new Error("Error getting blocklist: No data returned from database");
  }
  blob = data.data;
}

export function isUrlBlocked(url: string, flags: TeamFlags): boolean {
  // Self-hosted: never block any URL
  return false;
}
