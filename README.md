# KIS-RGI Cloud Bridge v2 — Railway / phone-only deployment

PC 없이 휴대폰 브라우저만으로 한국투자증권 Open API를 24시간 실행하기 위한 **읽기 전용** 브리지입니다.

## 반드시 필요한 값은 2개뿐
Railway `Variables`에 아래 두 값만 넣으면 됩니다.

- `KIS_APP_KEY`
- `KIS_APP_SECRET`

선택값 `KIS_DEMO=false`, `KIS_WS_SYMBOLS=005930`은 코드 기본값이 있어 생략 가능합니다.

## 하는 일
- KIS OAuth access token 자동 발급/재사용
- 국내주식 REST 현재가 조회
- WebSocket approval key 자동 발급
- `H0STCNT0` 국내주식 실시간 체결 구독
- WebSocket 자동 재연결
- REST / WebSocket 상태 조회
- RGI가 읽을 수 있는 HTTP bridge endpoint 제공

**주문/정정/취소/잔고 API는 포함하지 않습니다.** App Key/Secret 노출 시 주문 기능까지 연결되는 위험을 줄이기 위한 설계입니다.

## 휴대폰 배포 절차
1. 이 ZIP을 내려받아 휴대폰에서 압축을 풉니다.
2. GitHub에서 새 repository를 하나 만들고 이 폴더 안 파일들을 업로드합니다.
3. Railway에서 `New Project` → `Deploy from GitHub repo` → 방금 만든 repo 선택.
4. Railway Service → `Variables`에서 `KIS_APP_KEY`, `KIS_APP_SECRET` 입력.
5. Service → Settings → Networking에서 `Generate Domain`.
6. Deploy가 끝나면 `https://발급도메인/health` 접속.
7. `https://발급도메인/kis/smoke` 접속.
8. `ok:true`, `kis_rest_verified:true`와 삼성전자 현재가가 나오면 REST 연결 성공.
9. `https://발급도메인/kis/realtime/status`에서 `connected:true` 확인.
10. 장중이면 `https://발급도메인/kis/realtime/005930`에서 마지막 실시간 체결 확인.

Railway는 루트 `Dockerfile`을 자동 감지하며 `/health`를 healthcheck로 사용합니다. 서버는 Railway가 주입하는 `$PORT`에 자동 바인딩됩니다.

## RGI 연결에 사용할 주소
배포 후 생성된 Railway 도메인 하나만 ChatGPT에 알려주면 됩니다. **App Key/Secret은 알려주지 마세요.**

예:
- `/health`
- `/kis/smoke`
- `/kis/price/005930`
- `/kis/realtime/status`
- `/kis/realtime/005930`
- `/rgi/status`

## 실시간 종목 추가
기본으로 삼성전자 `005930`을 구독합니다.

HTTP POST:
`/kis/realtime/subscribe/000660`

삭제는 HTTP DELETE:
`/kis/realtime/subscribe/000660`

Railway Variables에 `KIS_WS_SYMBOLS=005930,000660,373220`처럼 넣어 시작 시 여러 종목을 자동구독할 수도 있습니다.

## 보안
- 실제 키를 GitHub, README, 코드, 채팅에 넣지 마세요.
- 키는 Railway Variables에만 저장하세요.
- 이전에 채팅/공개 장소에 노출한 키라면 재발급 후 사용하세요.
- 이 서버에는 주문 API가 없습니다.
- 공개 도메인에는 시장데이터 조회 endpoint만 노출됩니다.

## 연결 판정
- `/health` 성공 = 서버가 살아 있음
- `/kis/smoke` 성공 = App Key/Secret + OAuth + KIS REST가 실제로 연결됨
- `/kis/realtime/status`의 `connected:true` = KIS WebSocket 연결 성공
- `/kis/realtime/{symbol}` 응답 = 해당 종목의 실제 실시간 체결 수신 확인

## 현재 RGI 상태
브리지 상태 endpoint는 `rgi v.261007_13 / rgis v.261007_13`을 표시합니다. 브리지가 연결되었다고 해서 K2~K11 전체 broad-market feature assembly가 자동으로 완료됐다는 뜻은 아닙니다. 다음 단계에서 이 bridge를 RGI K2/K4/K5 collector에 연결합니다.
