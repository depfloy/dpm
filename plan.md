# DPM Bug Hunt — Fix Planı

Bug avı sonucu doğrulanmış gerçek bug'lar ve fix'leri. Yanlış pozitifler ve ölü koddaki bulgular elendi.

Kök neden deseni: `m.mu` kilidi ile bloklayan `stopProcess` işleminin ilişkisinin tutarsız olması.

## Görevler

- [x] **#2+#3 [YÜKSEK] Kök eşzamanlılık refactoru** — `stopProcess` signal (kilit altında: stopCh kapat + status) / wait (kilitsiz: kill/wait/port) olarak ayrıldı (`signalStop` helper'ı eklendi). `Stop()` ve `Drain()` artık sinyali kilit altında, bloklayan kill'i kilit dışında yapıyor; `Deploy` cleanup/rollback ve `startInstance` çağrıları da güncellendi.
  - `Stop()` global kilidi `stop_timeout` (10s) boyunca tutuyordu → liveness bug ✓ giderildi
  - `Drain()` kilidi bırakıp `proc.status`'u kilitsiz yazıyordu → veri yarışı + double-close ✓ giderildi
- [x] **#1 [YÜKSEK] Stop, restart backoff penceresinde yok sayılıyor** — `monitor()` `time.Sleep(delay)` sonrası `startInstance` öncesi `stopCh`'ı tekrar kontrol ediyor. Yeni test `TestStopDuringRestartBackoff` fix olmadan FAIL, fix ile PASS olduğu doğrulandı.
- [x] **#5 [ORTA] monitorAdopted başarısız restart'ta süreci kaybediyor** — `startInstance` hatasında proc StatusErrored ile map'e geri konuyor + persist + notify.
- [x] **#4 [ORTA-YÜKSEK] adoptOrphans çok-worker'lı uygulamayı tek worker'a düşürüyor** — base isim başına TÜM portlar toplanıp `Start(cfg, ports)` çağrılıyor. Yeni test `TestAdoptOrphansRestartsAllWorkerPorts` gerçek `adoptOrphans`'ı çağırıp 2 worker'ı doğruluyor.
- [x] **#6 [ORTA] Log rotation veri kaybı** — `compressTo` helper'ı `io.Copy`/`gz.Close`/`dst.Close` hatalarını kontrol ediyor, kısmi çıktıyı siliyor; sıkıştırma artık senkron (async rotate yarışı ortadan kalktı), orijinal yalnızca yedek dayanıklı yazıldıktan sonra siliniyor. Yedek isimlendirmesi tutarlı hale getirildi. Yeni test: `rotation_test.go`.
- [x] **#7 [ORTA-DÜŞÜK] Health exec check child süreç sızıntısı** — `exec.CommandContext` + `cmd.Cancel` ile process group öldürülüyor; ayrıca goroutine+select kaldırılarak `cmd.Process` üzerindeki (önceden var olan) veri yarışı da giderildi. Yeni test: `checker_test.go`.
- [x] **#8 [DÜŞÜK] Health checker map'leri temizlenmiyor** — `StopMonitoring` `doneChs`/`statuses` siliyor; `StartMonitoring` eski done kanalını kilit altında yakalıyor. Test: `TestStopMonitoringCleansMaps`.
- [x] **Doğrulama** — `go vet ./...` temiz; tüm paketler `go test ./...` ile PASS; dokunulan paketler (`log`, `health`, `state`, `config`, `daemon`) `-race` ile PASS; yeni testler eklendi ve #1 fix olmadan FAIL/fix ile PASS doğrulandı.

## Notlar

- **Önceden var olan test flake'i (bizim değişikliğimizle ilgisiz):** `internal/process` paketinin tamamı `-race` ile çalıştırıldığında ara sıra bir yarış raporluyor. Master'da (değişikliklerimiz stash'liyken) de tekrarlandı ve her seferinde farklı test patladı — sebep, bazı testlerin monitor goroutine'lerinin bitmesini beklemeden `store.Close()` çağırması (test izolasyon sorunu, üretim kodu değil). Ayrı bir iş olarak ele alınmalı.
- **Bonus:** #7 üzerinde çalışırken `checkExec` içinde önceden var olan bir `cmd.Process` veri yarışı tespit edilip düzeltildi.

## Kapsam Dışı (bilgilendirme)

- **Ölü kod:** `internal/deploy/orchestrator.go` ve nginx `MarkWorkerDown/MarkWorkerUp` hiçbir yerden çağrılmıyor. Ayrı temizlik işi.
- **Elenen yanlış pozitifler:** `dpmd` arg parsing (kod doğru), `StartMonitoring` stale-channel (doğru senkronizasyon), callback result-pointer yarışı (per-iteration local).

---

# DP-92 — dpmd deadlock (cmd.Wait inherited-pipe hang)

Canlı incident (ursa-jupiter, 2026-06-09): `sh -c` ile başlatılan süreçlerin torunları stdout/stderr pipe'ını miras alıp açık tutuyor → `io.Copy` EOF almıyor → `cmd.Wait()` sonsuza asılıyor → Manager mutex'i zehirleniyor → tüm API timeout. (DoD #1 — kilidin `Wait` boyunca tutulmaması — yukarıdaki #2+#3 refactoru ile zaten giderilmişti.)

Seçilen yaklaşım: pipe ve feature'lar (timestamp/gruplama/rotation) korunur; hang `WaitDelay` ile kırılır (DoD #3 = pipe'ı tamamen kaldırma bilinçli ertelendi).

- [x] **WaitDelay (DoD #2)** — `ProcessConfig.WaitDelay` (`wait_delay`, default 10s) eklendi; `startInstance` her `cmd`'de `cmd.WaitDelay = resolveTimeout(cfg.WaitDelay, 10s)` set ediyor. Süreç exit ettikten sonra pipe FD'lerini zorla kapatıp `cmd.Wait()`'in dönmesini garanti ediyor (hem `monitor` hem `stopProcess` yolu).
- [x] **stopProcess ikinci Wait düzeltmesi** — timeout dalındaki hatalı ikinci `cmd.Wait()` kaldırıldı; `Kill(-pgid, SIGKILL)` sonrası `done` kanalı `WaitDelay+2s` sert tavanıyla bekleniyor → `stopProcess` asla sonsuza bloklanmıyor.
- [x] **Setpgid + Kill(-pgid) (DoD #4), killPortHolder (DoD #5)** — zaten mevcuttu; doğrulandı.
- [x] **Regresyon testi (DoD #6)** — `TestStopReturnsWhenGrandchildHoldsPipe`: `sleep 300 &` ile torun pipe'ı açık tutuyor; WaitDelay olmadan süreç zombie olarak sonsuza "online" kalıp test FAIL, fix ile "stopped" + Stop/List responsive → PASS. Discriminasyon doğrulandı.
- **DoD #3 (pipe → dosya):** ERTELENDİ — kullanıcı kararı; feature regresyonu gerektiriyor (`timestampWriter` + `RotatingWriter` çıplak `*os.File` gerektirir).
- **DoD #7 (test sunucu + fleet rollout):** kod kapsamı dışı (deployment).
- Not: `router.go` değişmedi — wedge'in kaynağı `Stop()`'un kilit davranışıydı, Manager tarafında çözüldü.

---

# v1.8.1 — Blue-green deploy state-clobber (orphan-after-restart) + stopProcess double-Wait

Production gözlem (ursa-jupiter test env): update sonrası `dpm list` boş ama süreçler arka planda canlı. Log kanıtı: state DB, 1.7.0→1.7.1 arasında (2026-04-27'deki 6 blue-green deploy'dan sonra) boşaldı; 1.7.1 ve 1.8.0 açılışta "no orphan processes to adopt". **v1.8.0 sebep değil** — DB zaten bir aydır boştu.

- [x] **Blue-green deploy store-clobber (asıl kök neden)** — `Deploy` adım 4 yeni worker'ı `finalKey`'e persist ediyor, ama adım 5 eski worker'ı park ederken **aynı finalKey'i** store'dan siliyordu (eski+yeni aynı isimde aynı anahtarı paylaşır). Sonuç: canlı süreç store'dan düşüyor → bir sonraki restart'ta orphan. Fix: adım 4'te persist edilen `finalKeys` set'i; adım 5'te bu set'teki anahtar silinmiyor (`manager.go` `Deploy`).
- [x] **stopProcess double-Wait veri yarışı (bonus, pre-existing)** — `stopProcess` cmd-backed dalı `go cmd.Wait()` açıyordu; ama `monitor` goroutine'i zaten aynı `*exec.Cmd` üzerinde `cmd.Wait()` çağırıyor → eşzamanlı çift Wait = data race. Fix: `stopProcess` artık Wait çağırmıyor, `waitForExit` ile `processAlive` polling yapıyor; tek Wait sahibi `monitor` (WaitDelay ile reap eder). Bu, process paketindeki eski "-race flake"in başlıca kaynağıydı.
- [x] **Regresyon testi** — `TestDeployPersistsPromotedWorkerInStore`: deploy sonrası `store.GetProcess(finalKey)` yeni port ile kalmalı. Fix olmadan "process not found" → FAIL, fix ile PASS. İzole `-race` temiz.
- [x] **Test-only `-race` flake düzeltildi** — `TestReloadAllWithAdoptedProcesses` canlı bir process'in `proc.cmd`'ını elle `nil` yapıp monitor ile yarışıyordu. Test artık gerçek adoption yolunu kullanıyor: harici bir `sleep` process'i spawn edip `mgr.Attach` ile adopt ediyor (cmd=nil + `monitorAdopted`, `proc.cmd`'a hiç dokunmaz). `process` paketi artık `-race` ile temiz.
