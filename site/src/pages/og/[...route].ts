import { getCollection } from "astro:content";
import { OGImageRoute } from "astro-og-canvas";

// Load all docs from Starlight's content collection
const docs = await getCollection("docs");

// Map to { slug: { title, description } }
const pages = Object.fromEntries(
	docs.map((entry) => [
		entry.id,
		{
			title: entry.data.title,
			description: entry.data.description ?? "",
		},
	]),
);

export const { getStaticPaths, GET } = await OGImageRoute({
	pages,

	getImageOptions: (_path, page: { title: string; description: string }) => ({
		title: page.title,
		description: page.description,

		// Brand colors matching landing page
		background: {
			type: "linear",
			angle: 135,
			stops: [
				[0, "#0a0e14"],
				[1, "#111720"],
			],
		},

		padding: 80,

		// Logo (PNG — canvaskit doesn't support SVG)
		logo: {
			path: "./src/assets/logo.png",
			size: [48],
		},

		// Title styling
		font: {
			title: {
				color: [230, 237, 243], // #e6edf3
				size: 64,
				weight: "Bold",
				lineHeight: 1.2,
				families: ["Inter"],
			},
			description: {
				color: [139, 148, 158], // #8b949e
				size: 32,
				weight: "Normal",
				lineHeight: 1.4,
				families: ["Inter"],
			},
		},

		// Load Inter font (local files — not CSS @import URL)
		fonts: [
			"./src/assets/fonts/Inter-Regular.woff2",
			"./src/assets/fonts/Inter-Bold.woff2",
		],

		// Border accent
		border: {
			color: [0, 212, 170], // #00d4aa
			side: "inline-start",
			width: 6,
		},
	}),
});
