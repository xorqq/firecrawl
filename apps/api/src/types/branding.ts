export interface BrandingProfile {
  logo?: string | null;
  fonts?: Array<{
    family: string;
    [key: string]: unknown;
  }>;
  colors?: {
    primary?: string;
    secondary?: string;
    accent?: string;
    background?: string;
    text_primary?: string;
    text_secondary?: string;
    link?: string;
    success?: string;
    warning?: string;
    error?: string;
    [key: string]: string | undefined;
  };
  typography?: {
    font_families?: {
      primary?: string;
      secondary?: string;
      code?: string;
      [key: string]: string | undefined;
    };
    font_sizes?: {
      h1?: string;
      h2?: string;
      h3?: string;
      body?: string;
      small?: string;
      [key: string]: string | undefined;
    };
    line_heights?: {
      heading?: number;
      body?: number;
      [key: string]: number | undefined;
    };
    font_weights?: {
      light?: number;
      regular?: number;
      medium?: number;
      bold?: number;
      [key: string]: number | undefined;
    };
  };
  spacing?: {
    base_unit?: number;
    padding?: Record<string, number>;
    margins?: Record<string, number>;
    grid_gutter?: number;
    border_radius?: string;
    [key: string]: number | string | Record<string, number> | undefined;
  };
  components?: {
    buttons?: Record<
      string,
      {
        background?: string;
        text_color?: string;
        hover_background?: string;
        border_radius?: string;
        border?: string;
        [key: string]: string | undefined;
      }
    >;
    inputs?: {
      border_color?: string;
      focus_border_color?: string;
      border_radius?: string;
      [key: string]: string | undefined;
    };
    cards?: {
      background?: string;
      shadow?: string;
      border_radius?: string;
      [key: string]: string | undefined;
    };
    [key: string]: unknown;
  };
  icons?: {
    style?: string;
    primary_color?: string;
    [key: string]: string | undefined;
  };
  images?: {
    logo?: string | null;
    favicon?: string | null;
    default_og_image?: string | null;
    [key: string]: string | null | undefined;
  };
  animations?: {
    transition_duration?: string;
    easing?: string;
    [key: string]: string | undefined;
  };
  layout?: {
    grid?: {
      columns?: number;
      max_width?: string;
      [key: string]: number | string | undefined;
    };
    header_height?: string;
    footer_height?: string;
    [key: string]:
      | number
      | string
      | Record<string, number | string | undefined>
      | undefined;
  };
  tone?: {
    voice?: string;
    emoji_usage?: string;
    [key: string]: string | undefined;
  };
  [key: string]: unknown;
}
