# İYE — Yapılacaklar

## Orta Vadeli

- [ ] **E2E (End-to-End) Test** — Tüm pipeline'ı baştan sona test eden entegrasyon testi:
      1. Geçici log dosyası oluştur
      2. Tailer ile oku
      3. Masker, Metrics, Sampling, Buffer'dan geçir
      4. Transport ile `httptest.Server`'a gönder
      5. Format, compression, field doğrulaması yap
- [ ] **Benchmark testleri** — Go `testing.B` ile component bazlı performans testleri (buffer EPS, masking throughput, sampling latency)
- [ ] **Yük testi (simülasyon)** — Aşağıdaki araçlardan biriyle transport endpoint'ine yük testi:
      - **k6 (Grafana)** — Endüstri standardı, Go+JS, gerçekçi kullanıcı simülasyonu, CI entegrasyonlu
      - **Vegeta** — Hafif HTTP yük testi, özellikle transport endpoint'i için ideal
      - **f1 (Form3)** — Native Go load testing framework, component seviyesinde test için
      - **`go test -bench`** — Mikro-benchmark'lar için (buffer, masker)
- [ ] **Chaos test** — Beklenmedik durumlar testi: disk dolar, ağ kesilir, process kill yeniden başlatılır
- [ ] **Sanal alan (sandbox) ile tailer testlerini iyileştirme**
- [ ] **Gösterge paneli (dashboard) metrik görselleştirmesi için eklenti geliştirme**

## Uzun Vadeli

- [ ] **Protobuf serileştirme** — JSON yerine protobuf (daha küçük, daha hızlı, transport ile tutarlı)
- [ ] **mTLS desteği** — Mutual TLS (karşılıklı sertifika doğrulama)
- [ ] **Docker imaj CI** — GitHub Actions workflow ile otomatik imaj oluşturma
- [ ] **Yapılandırma hot-reload** — Sinyal ile yeniden yükleme
- [ ] **Rate limiting** — Transport katmanında gönderim hız sınırlandırma
- [ ] **Birden çok transport hedefi** — Aynı anda birden çok endpoint'e gönderme

## Simülasyon/Yük Testi Araştırması

| Araç | Dil | Ne İşe Yarar | İYE için Kullanım |
|------|-----|--------------|-------------------|
| **k6** | Go + JS | HTTP yük testi, senaryo tabanlı | Transport'a 10K+ RPS gönderip davranışı gözlemleme |
| **Vegeta** | Go | Hafif HTTP yük aracı | Transport endpoint'ine sabit RPS'de saldırı testi |
| **f1** | Go | Go kodu ile load test framework'ü | Pipeline component'lerini direkt Go kodu ile test etme |
| **built-in benchmark** | Go | `testing.B` ile mikro benchmark | Masker, buffer, sampling hız ölçümü |

**Öneri:** İlk adımda `go test -bench` ile component benchmark'ları yeterli. Yük testi ihtiyacı doğduğunda k6 en profesyonel seçenek.
