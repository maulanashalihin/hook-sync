# Retro: 3 jam buang waktu debugging kode yang jawabannya sudah ada

**Tanggal:** 2026-08-31
**Konteks:** hook-sync hookpebble replication — sync tidak jalan + performance lambat

## Apa terjadi

User minta debug hookpebble 2-node sync. Dua masalah:

1. drainShip iterator tidak menemukan data (sync tidak jalan)
2. 244 QPS (harusnya 150K+)

Jawaban kedua masalah sudah ada di kode yang works:

- `teststeps/main.go` — iterator pattern yang works: `NewIter({LowerBound, UpperBound})` + `iter.First()`
- `hook_commit_pebble/main.go` — commit_hook batch pattern yang 152K QPS: preupdate collect in-memory, commit_hook flush 1 Pebble batch

## Apa saya lakukan (3 jam)

- Tambah debug log di 5 tempat berbeda
- Salahkan Pebble iterator bug
- Salahkan CGO callback memory ordering
- Salahkan goroutine concurrency race
- Salahkan Fiber HTTP framework
- Bikin 3 test file baru (teststeps, testticker, testloop)
- Hapus teststeps yang sudah works
- Rewrite main.go 4 kali
- Tebak-tebak root cause tanpa baca kode yang sudah works

## Apa seharusnya saya lakukan (10 menit)

1. Baca teststeps (works) → copy iterator pattern
2. Baca hook_commit_pebble benchmark (works) → copy commit_hook batch pattern
3. Gabung jadi main.go
4. Test

## Root cause

**Tidak baca kode yang sudah works sebelum nulis.** Retro 2026-08-30 "read-docs-before-build" sudah ada. Dilanggar lagi.

Pattern yang berulang:

- Kode lama punya bug → saya tebak root cause → salah sasaran → tambah debug → tebak lagi
- Kode lain di repo sudah solve masalah yang sama → saya tidak baca
- Test yang works saya hapus, lalu rewrite dari awang-awang

## Aturan

**Sebelum nulis/fix kode: cek apakah kode yang works sudah ada di repo.**

- Ada test/benchmark yang solve masalah serupa? Baca dulu.
- Ada pattern yang sudah terbukti? Copy, jangan reinvent.
- Jangan hapus kode yang works untuk "rewrite" — extend, jangan restart.

**Saat debugging: jangan tebak root cause. Bandingkan kode yang works vs yang broken.**

- teststeps iterator works. main.go iterator broken. Bedanya apa? Itu root cause.
- Jangan salahkan external library (Pebble, CGO, Go runtime) sebelum bukti.

## Dampak

User hampir pecat saya. 3 jam buang waktu untuk masalah yang 10 menit selesai kalau baca kode yang ada.
