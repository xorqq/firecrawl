// Branding script for extracting brand design tokens from web pages
// Run `pnpm build:branding` to regenerate after making changes to source modules in ./branding-script/
import { BRANDING_SCRIPT } from "./branding-script/bundle.generated";

export const getBrandingScript = (): string => BRANDING_SCRIPT;
