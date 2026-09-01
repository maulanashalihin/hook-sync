// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
	site: 'https://hook-sync.pages.dev',
	integrations: [
		starlight({
			title: 'hook-sync',
			description: 'SQLite replication that just works. Multi-server, multi-writer, multi-runtime. Zero data loss.',
			logo: {
				src: './src/assets/logo.svg',
				alt: 'hook-sync',
			},
			customCss: [
				'./src/styles/theme.css',
			],
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/maulanashalihin/hook-sync' },
				{ icon: 'npm', label: 'npm', href: 'https://www.npmjs.com/package/hooksync.js' },
			],
			sidebar: [
				{
					label: 'Getting Started',
					items: [
						{ label: 'Overview', slug: 'getting-started/overview' },
						{ label: 'Quick Start', slug: 'getting-started/quick-start' },
						{ label: 'How It Works', slug: 'getting-started/how-it-works' },
					],
				},
				{
					label: 'Libraries',
					items: [
						{ label: 'Go Libraries', slug: 'libraries/go' },
						{ label: 'JS Library (hooksync.js)', slug: 'libraries/js' },
					],
				},
				{
					label: 'Topologies',
					items: [
						{ label: 'Point-to-Point', slug: 'topologies/point-to-point' },
						{ label: 'Full Mesh', slug: 'topologies/full-mesh' },
						{ label: 'Dedicated Hub', slug: 'topologies/hub' },
						{ label: 'Multi-Region', slug: 'topologies/multi-region' },
					],
				},
				{
					label: 'Reference',
					items: [
						{ label: 'Wire Protocol', slug: 'reference/protocol' },
						{ label: 'Benchmarks', slug: 'reference/benchmarks' },
						{ label: 'Split-Brain Safety', slug: 'reference/split-brain' },
					],
				},
			],
		}),
	],
});
