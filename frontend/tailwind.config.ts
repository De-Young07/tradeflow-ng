import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        green: {
          DEFAULT: "#1A6B3C",
          dark:    "#0D1F14",
          mid:     "#1A6B3C",
          light:   "#2A8B52",
          faint:   "#E8F5EE",
        },
        gold: {
          DEFAULT: "#C8860A",
          light:   "#E8A020",
          faint:   "#FEF6E4",
        },
        cream: {
          DEFAULT: "#F5F2EB",
          dark:    "#EAE6DC",
        },
      },
      fontFamily: {
        heading: ["var(--font-jakarta)", "sans-serif"],
        body:    ["var(--font-inter)",   "sans-serif"],
      },
      boxShadow: {
        card: "0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04)",
        "card-hover": "0 4px 12px rgba(0,0,0,0.10)",
      },
      borderRadius: {
        xl:  "12px",
        "2xl": "16px",
      },
    },
  },
  plugins: [],
};

export default config;
