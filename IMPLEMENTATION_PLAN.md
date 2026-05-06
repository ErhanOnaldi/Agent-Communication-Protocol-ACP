# Implementation Plan — 4 Tasks

> **Context:** Backend (`kalori_backend_rust`) ve Frontend (`kalori_app` — Flutter) iki ayrı makinede iki ayrı AI ajanı tarafından geliştirilecek. Lokal ağ üzerinden HTTP ile haberleşiyorlar. Bu doküman her ajanın kendi sorumluluk alanını kendi başına anlayıp uygulayabileceği şekilde yazıldı.
>
> **Kapsam:** 5 madde içinden Madde 4 (Fasting) iptal edildi. Madde 3 için karar: **Option C** — tek glass capsule, içinde iki ayrı tap zone (streak rakamı + ayar ikonu), aralarında divider.
>
> **Orchestration kuralı:** Her görev `[FE]` (frontend), `[BE]` (backend) veya `[FE+BE]` (koordineli) olarak işaretli. `[FE+BE]` görevlerde "Contract" bölümü iki taraf için de bağlayıcıdır — biri değiştirirse karşı tarafı bilgilendirmeli.

---

## Repo / dizin haritası

| Repo | Dil | Path (lokal) | Sorumluluk |
|------|-----|--------------|-----------|
| `kalori_app` | Flutter/Dart | `CalorieTrackingApp/kalori_app/` | UI, state, local-first cache, HealthKit native bridge |
| `kalori_backend_rust` | Rust/Axum | `kalori_backend_rust/` | API (3000), embedding sidecar (4000), Postgres |

Frontend backend URL'sini `--dart-define=KALORI_BACKEND_BASE_URL=...` ile alır. Lokal ağda backend makinesinin IP'si verilir.

---

## TASK 1 — Weekly Stats Layout & Chart Fixes  `[FE]`

**Tip:** Pure frontend. Backend değişikliği YOK.

### 1.1 Hafta navigasyon okları çok yakın
- **Dosya:** [kalori_app/lib/screens/stats/weekly_stats_screen.dart](kalori_app/lib/screens/stats/weekly_stats_screen.dart)
- **Satır:** 237 — `Padding(horizontal: navBtnSize + (isNarrow ? 6 : 8))`
- **Yapılacak:** Padding mantığı okları date label'a yapıştırıyor. Yatay padding'i azalt; ok butonları ile içerik arasına `Spacer()` veya min `48px` sabit boşluk koy. `_NavBtn` boyutu (satır 232) ile aralarındaki track aralığı ayrı değişkenler olsun.
- **Test:** Dar ekran (iPhone SE) ve geniş ekran (Pro Max) snapshot.

### 1.2 Maskot için fazla aşağı kayma
- **Dosya:** aynı, satır 165–168 (`navLaneHeight`), 202–203 (`Positioned(top: -8, …)`)
- **Yapılacak:** Maskotu `Stack`'ten çıkar veya negatif `top` ile parent layout'ı şişirme. Maskot `Overlay`/`Positioned` ile header'ı **overlap** etsin, header `navLaneHeight`'ı maskot için pad'lenmesin.
- **Hedef:** Header yüksekliği maskot yokmuş gibi normal kalsın, maskot üstüne taşsın.

### 1.3 Bar chart altında gün sayıları yok
- **Dosya:** [kalori_app/lib/screens/stats/stats_charts.dart](kalori_app/lib/screens/stats/stats_charts.dart)
- **Satır:** 18 (`_kWeekDays`), 40–75 (`_bottomWeekdayLabel`)
- **Yapılacak:** `_bottomWeekdayLabel`'ı iki satırlı yap — üstte gün harfi (`M`, `T`, …), altta ay-günü (`6`, `7`, …). Tarih hesabı için chart'a `weekStartDate: DateTime` parametresi ekle, index ile `weekStartDate.add(Duration(days: i))` üzerinden gün numarası hesapla.
- **Etkilenen chart'lar:** `StatsCalorieBarChart`, `StatsSodiumBarChart`, ve diğer haftalık chart'lar — hepsinin `_bottomWeekdayLabel` callback'i güncellenmeli.

### 1.4 Sodium 4 haneli sayılar sığmıyor
- **Dosya:** aynı, `StatsSodiumBarChart` satır 741–845
- **Satır:** 798 (`reservedSize: 32`), 803 (`'${v.toInt()}'`)
- **Yapılacak:**
  - `reservedSize`'ı 40 → 44 yap.
  - Y-axis label formatter'ı, calorie chart'taki (168–170) mantıkla:
    ```dart
    String _formatSodium(double v) =>
      v >= 1000 ? '${(v / 1000).toStringAsFixed(1)}k' : '${v.toInt()}';
    ```
  - `_leftAxisLabel`'a `maxLines: 1` + `overflow: TextOverflow.visible` (kırpma yerine taşma).

### Test (FE)
```bash
cd kalori_app && flutter analyze && flutter test test/  # full suite (no specific weekly test exists)
```

### Sonuç istenen gibi olmazsa kontrol listesi
- Oklar hâlâ yakın → `Stack` `Positioned.fill`, `boundedNavTrackWidth`, `Align.centerLeft/centerRight` (weekly_stats_screen.dart 230–260).
- Maskot hâlâ kayık → [kalori_app/lib/widgets/mascot_registry.dart](kalori_app/lib/widgets/mascot_registry.dart) içindeki `entrance`/`scene` y-translate değerleri.
- Sodium hâlâ taşıyor → fl_chart `SideTitlesData.reservedSize` ile parent `LayoutBuilder` width arasındaki conflict; `_leftAxisLabel` overflow ayarı.
- Gün sayısı yanlış güne denk → `weekStartDate` lokalizasyonu (`UserDataService.weeklyCaloriesSeries()` startOfWeek hesabı).

---

## TASK 2 — Apple Health Sync Charts'a Yansımıyor  `[FE+BE]`

**Tip:** Asıl sorun frontend tarafında (sync sonrası pull yok), ama backend payload'ı **macro nutrient eksik** olduğu için sodium grafiği için backend genişletme gerekebilir. İki ajan da aynı anda çalışabilir, koordinasyon noktası: **Contract** bölümü.

### Tanı (mevcut akış)
1. Toggle açıldığında frontend `connectAndSync()` çağırıyor → `POST /api/health-sync` (steps + active_energy + weight gönderiyor).
2. Backend `body_metrics`, `health_syncs`, `health_daily_summaries` tablolarına yazıyor.
3. **Frontend grafikleri** `UserDataService.daySummaryForDate()` cache'inden okuyor — bu cache `/api/v1/export` ile populate oluyor.
4. **Eksik link:** Toggle başarılı olduktan sonra frontend `/api/v1/export`'ı yeniden çağırmıyor → cache eski → grafik boş.
5. **İkinci eksik:** Backend `health_daily_summaries` şu an sadece `step_count`, `active_energy_burned_kcal` saklıyor. Sodium yok → sodium chart'ı için Apple Health'ten sodium fetch + backend persist + export içinde sodium gerekli.

### 2A `[FE]` Sync sonrası full-sync tetikle (ÖNCELİKLİ — bunu yaparsak kalori/aktivite grafikleri düzelir)
- **Dosya:** [kalori_app/lib/screens/settings/settings_screen.dart](kalori_app/lib/screens/settings/settings_screen.dart)
- **Satır:** 126–159 (`_updateHealthSync`)
- **Yapılacak:**
  - `connectAndSync()` `success=true` döndüğünde sırayla:
    1. `BackendBridgeService.instance.bootstrapUserDataFromBackend()` veya direkt `SyncService.performFullSync()` (hangisi `/api/v1/export` çağırıyorsa).
    2. UI'da geçici "Senkronize ediliyor…" indicator.
  - `connectAndSync()` `lastError` döndüğünde toast/banner ile kullanıcıya göster.
- **iOS izin:** [kalori_app/ios/Runner/Info.plist](kalori_app/ios/Runner/Info.plist) içinde `NSHealthShareUsageDescription` olduğunu doğrula. [apple_health_sync_platform_io.dart](kalori_app/lib/services/apple_health_sync_platform_io.dart) içinde `requestAuthorization()` çağrısının istenen tüm tipleri kapsadığını kontrol et (steps, active energy, weight, **dietary_sodium** — 2B için).

### 2B `[FE]` HealthKit'ten sodium oku ve backend'e gönder
> 2C tamamlandıktan sonra çalışacak. Backend payload'ı genişledikten sonra.
- **Dosya:** [kalori_app/lib/services/apple_health_sync_service.dart](kalori_app/lib/services/apple_health_sync_service.dart) (~150+ satır), `apple_health_sync_platform_io.dart`
- **Yapılacak:**
  - Native iOS HealthKit query'sine `HKQuantityTypeIdentifier.dietarySodium` (mg) ekle.
  - Servis modeline `daily_nutrition_summaries: [{ summary_date, sodium_mg, … }]` ekle.
  - `_BackendBridgeAppleHealthClient.syncHealthProviderData` payload'ını yeni alanı içerecek şekilde genişlet.

### 2C `[BE]` Health-sync payload'ını nutrient'ları içerecek şekilde genişlet
- **Dosya:** Backend `kalori_backend_rust/src/` (health-sync handler ve schema)
- **Mevcut endpoint:** `POST /api/health-sync` (test referans: `tests/api_routes.rs:1749` `health_sync_upserts_metrics_and_tokens`)
- **Yapılacak:**
  - Request schema'ya opsiyonel `daily_nutrition_summaries` array'i ekle:
    ```json
    "daily_nutrition_summaries": [
      { "summary_date": "2026-04-09", "sodium_mg": 2380.0, "dietary_energy_kcal": 1850.0 }
    ]
    ```
  - DB: `health_daily_summaries` tablosuna `sodium_mg DOUBLE PRECISION NULL`, `dietary_energy_kcal DOUBLE PRECISION NULL` kolonları ekle (migration).
  - Upsert mantığı: aynı `(user_id, summary_date, platform)` için merge.
  - **Backwards compat:** mevcut payload (sadece activity + weight) geldiğinde 200 dönmeye devam et — yeni alan opsiyonel.
  - Test ekle: `tests/api_routes.rs` içinde `health_sync_persists_nutrition_summaries`.

### 2D `[BE]` `/api/v1/export` cevabına sodium dahil et
- **Dosya:** Backend export handler (`/api/v1/export`)
- **Yapılacak:** `health_daily_summaries` array'inde her satırda `sodium_mg`, `dietary_energy_kcal` alanları yer alsın. (Test referans: `tests/api_routes.rs:3857` mevcut export assertion).

### 2E `[FE]` Export'tan gelen sodium'u `UserDataService` summary'sine map et
- **Dosya:** [kalori_app/lib/services/sync_service.dart](kalori_app/lib/services/sync_service.dart) (yoksa benzer ad), `user_data_service.dart`
- **Yapılacak:** Export response parse'ına yeni alanı ekle. `UserDailyNutritionSummary` modelinde `sodiumMg` zaten varsa (kontrol et) Apple Health kaynaklı satırı manuel girilen üstüne ezme — manual üstün olsun (referans: backend test `health_sync_does_not_override_manual_body_metric` satır 1873).

### Contract (FE+BE birlikte uymalı)

```
POST /api/health-sync   (genişletilmiş)
{
  "user_id": uuid,
  "platform": "apple_health",
  "access_token": string?,
  "daily_activity_summaries": [{summary_date, step_count, active_energy_burned_kcal}]?,
  "weight_records": [{logged_date, weight_kg}]?,
  "daily_nutrition_summaries": [          // YENİ — opsiyonel
    {
      "summary_date": "YYYY-MM-DD",
      "sodium_mg": float?,
      "dietary_energy_kcal": float?
    }
  ]?
}
→ 200 { "synced_count": N }

GET /api/v1/export
{
  ...
  "health_daily_summaries": [
    {
      "summary_date": "...",
      "step_count": int?,
      "active_energy_burned_kcal": float?,
      "sodium_mg": float?,                // YENİ
      "dietary_energy_kcal": float?       // YENİ
    }
  ]
}
```

### Test
- **BE:** `cargo test health_sync` — yeni nutrition test'i + mevcut testlerin geçtiğini doğrula.
- **FE:** Manuel — toggle aç, charts'ın 2 saniye içinde dolduğunu izle. iOS'ta HealthKit izin dialog'u çıkmalı.

### Sonuç istenen gibi olmazsa kontrol listesi
- Toggle açık ama provider listesi boş → `GET /api/v1/health-sync/providers` cevabı (`profile_service.dart:122`). Backend `health_syncs` insert düşmemiş olabilir → BE log.
- 200 dönüyor ama grafik hâlâ boş → `bootstrapUserDataFromBackend` sonrası `UserDataService.upsertDailySummary` çağrılıyor mu? FE log.
- HealthKit izin dialog'u çıkmıyor → `Info.plist` + `requestAuthorization()` çağrısının istenen tipleri kapsayıp kapsamadığı.
- Sadece sodium boş → 2B/2C/2D zinciri tamamlandı mı? Backend payload'ı ham JSON ile manual `curl` ile gönderip body_metrics tablosuna düştüğünü doğrula.
- Manuel girilen sodium üstüne yazılıyor → BE upsert mantığı `does_not_override_manual` testini ihlal ediyor.

---

## TASK 3 — Streak + Settings Tek Capsule (Option C)  `[FE]`

**Tip:** Pure frontend. Backend yok.

**Karar:** Tek glass capsule, içinde iki tap zone — sol: streak rakamı (→ StreakScreen), sağ: ayar ikonu (→ SettingsScreen). Aralarında ince divider.

- **Dosya:** [kalori_app/lib/widgets/top_header.dart](kalori_app/lib/widgets/top_header.dart) satır 148–241
- **Yapılacak:**
  1. Mevcut iki ayrı `Container` (154–205 streak, 207–238 settings) ve aralarındaki `SizedBox(width: 8)` (206) yerine **tek glass `Container`**:
     ```
     Container (glass background, ortak border-radius)
       └─ Row(mainAxisSize: min)
           ├─ InkWell(onTap: onStreakTap, customBorder: leftHalfRadius)
           │    └─ Padding + Row[Icon(fire), Text(streakDays)]
           ├─ Container(width: 1, height: ~16, color: divider) // VerticalDivider yerine
           └─ InkWell(onTap: () => SettingsScreen.show(context), customBorder: rightHalfRadius)
                └─ Padding + Icon(settings)
     ```
  2. `InkWell` ripple'ları kendi yarı capsule'larında kalsın (`customBorder` ile yarım `RoundedRectangleBorder`).
  3. `main_layout.dart:228–230` callback wiring değişmiyor (`onStreakTap` korunur, settings hâlâ in-widget).

- **Görsel detay:** Divider opak değil, glass background üstünde `Colors.white.withOpacity(0.18)` gibi ince bir çizgi.
- **Test:** `flutter test` (top_header için widget test varsa güncelle).

### Sonuç istenen gibi olmazsa kontrol listesi
- Glass efekt bozuldu → `kalori_theme.dart` glass token'ları (capsule arka planı + blur).
- Tap zone yanlış ekrana gidiyor → `InkWell.onTap` ile `MainLayout.onStreakTap` callback chain.
- Streak rakamı güncellenmiyor → `UserDataService.streakDays` notifier subscription, `top_header` rebuild scope'u.
- Ripple capsule dışına taşıyor → `customBorder` `RoundedRectangleBorder` half-radius doğru ayarlandı mı; `Material(clipBehavior: Clip.antiAlias)` parent gerekebilir.

---

## TASK 4 — Calendar Modal İç Yazıların Animasyon Sırasında Titremesi  `[FE]`

**Tip:** Pure frontend.

### Tanı
`AnimatedBuilder(animation: curved)` (calendar_modal.dart:521) calendar Hero container'ın **hem dış kabuğunu hem iç içeriğini** sarıyor. Hero flight + size transition sırasında container boyutu değişiyor → içerideki `Column` (ay başlığı + weekday labels + day cells grid) sürekli yeniden layout oluyor → text "hobbidi hobbidi" titriyor.

İstenen: Dış container "perde gibi" açılıp kapansın, iç içerik **sabit** dursun.

### Yapılacak
- **Dosya:** [kalori_app/lib/widgets/calendar_modal.dart](kalori_app/lib/widgets/calendar_modal.dart)
- **Satır aralığı:** 470–553 (`_CalendarHeroRoute.buildTransitions`), 260–391 (`_CalendarModalState.build` — Hero child)

**Yaklaşım: ClipRect + sabit boyutlu iç içerik (reveal pattern)**

1. İç `Column` içeriğini **final boyutta** layout et (`SizedBox(width: finalWidth, height: finalHeight)` ile sabitle).
2. İç içeriği `OverflowBox(maxWidth: double.infinity, maxHeight: double.infinity)` ile sar — parent constraint küçükken bile child tam boyutta layout olsun.
3. Dış container'ı `ClipRect` içine al; clip area animasyon ile genişlesin (perde efekti).
4. `AnimatedBuilder` artık sadece **clip rect'in size'ını** ve **container background opacity/scale'ini** drive etsin — child'ın layout'ına dokunma.
5. Day cell'lere `RepaintBoundary` ekle → re-paint maliyeti düşsün.

**Hero flight için:**
- `flightShuttleBuilder` (calendar_modal.dart:14–50) custom shuttle veriyorsa içeriği target widget'la **size-stable** olarak interpolate etsin. Default Hero cross-fade içeriği sallayabilir.
- Source Hero ([top_header.dart:100–144](kalori_app/lib/widgets/top_header.dart#L100-L144)) ile destination Hero arasında child widget tipi ve boyutu match olmalı, yoksa shuttle fallback cross-fade devreye girer.

**Ne yapma:**
- İç içeriğe `FadeTransition` ekleme. `FadeTransition` sadece barrier'da kalsın (mevcut 539–545 doğru).
- `AnimatedSize` veya `SizeTransition` doğrudan `Column`'u sarmasın — bunlar child'ı sürekli relayout yapar.

### Test
- Manuel — calendar açıp kapama animasyonunu yavaş çek (Flutter DevTools "slow animations") ve text'in stationary olduğunu gözle.
- iOS + Android iki platformda da kontrol.

### Sonuç istenen gibi olmazsa kontrol listesi
- Hâlâ titriyor → Hero `flightShuttleBuilder` source/destination size mismatch — DevTools "Highlight repaints" aç.
- ClipRect kestiği için içerik kırpılıyor → `OverflowBox(maxWidth: infinity)` constraint'i doğru mu; child Column `mainAxisSize: min` mi?
- Day cell opacity değişiyor → parent `Opacity`/`FadeTransition` chain'ini grep'le; `Material(type: transparency)` zaten doğru kullanılmış.
- Açılma süresi/curve istenmiyor → `_CalendarHeroRoute.transitionDuration` + `CurvedAnimation.curve`.
- Android'de farklı, iOS'ta düzgün → platform-specific Hero behavior; `PageRouteBuilder` `transitionsBuilder` ile elle drive et.

---

## Sıra önerisi (paralel + bağımlılık)

```
[FE Agent]                                  [BE Agent]
  ├─ Task 1 (Weekly stats)  ───┐
  ├─ Task 3 (Streak/settings) ─┤
  ├─ Task 4 (Calendar)        ─┤
  │                            │
  ├─ Task 2A (sync→full pull) ─┤   (bunlar paralel, 1 gün)
  │                            │
  │                            ▼
  │                          ┌────────────────────────┐
  │                          │ Task 2C+2D (BE schema, │
  │                          │ migration, export)     │
  │                          └────────────────────────┘
  │                                     │
  ▼                                     ▼ (contract sabitlendi)
Task 2B+2E (FE: HK sodium read + export parse)
```

**Kritik koordinasyon noktası:** Task 2 contract'ı (yukarıdaki "Contract" bölümü). BE migration'ı merge'lenmeden FE 2B/2E iş yapamaz; ama 2A backend'e dokunmadığı için derhal başlayabilir ve büyük olasılıkla kullanıcının "grafik boş" şikayetinin **çoğunu** çözer (kalori, weight, steps).

---

## Ortak komutlar

```bash
# Frontend
cd kalori_app
flutter analyze
flutter test
# integration run with backend defines:
./scripts/run_integration.sh   # KALORI_BACKEND_BASE_URL'i .env'den okur

# Backend
cd kalori_backend_rust
cargo test
cargo run --bin embedding_svc   # terminal 1
cargo run                       # terminal 2 (port 3000)
```

## Güvenlik / disiplin
- BE: secrets sadece env'den; migration'lar reversible olsun (down script).
- FE: `KALORI_BACKEND_BEARER_TOKEN` sadece dev fallback — prod build'de set edilmesin.
- Hiçbir taraf legacy redirect'leri kaldırmasın (`router.dart` deep-link migration'ı bilinçli yapılmadıkça).
