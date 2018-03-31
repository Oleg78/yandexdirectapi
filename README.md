# yandexdirectapi
Asynchronous client for Yandex Direct API

## Install
0. Read the Yandex manual: https://tech.yandex.ru/direct/doc/dg/concepts/about-docpage/
0. Register your app: https://tech.yandex.ru/direct/doc/dg/concepts/register-docpage/
0. Get the token: https://tech.yandex.ru/direct/doc/dg/concepts/auth-token-docpage/
0. Clone the repo
0. `pip install .`

## Usage
```python
from yandexdirectapi import DirectAPI5
api = DirectAPI5(YANDEX_DIRECT_LOGIN, YANDEX_DIRECT_TOKEN, 10)
campaigns = api.get_campaigns()
```

## Implemented methods:
- get_campaigns
- get_campaign_groups
- get_campaign_active_groups
- get_groups_bids
- get_campaign_bids
- get_campaigns_bids
- set_bids
