# Scroll target for Yahoo Finance topic news stream (JS: document.querySelector("#modular-block-news-stream")).
MODULAR_BLOCK_NEWS_STREAM_ID = "latest-news"

ARTICLE_GRIDLAYOUT_yf_cfn520 = "/html/body/div[2]/main/section/section/section/article"
SECTION_TOPICHERO_yf_rxsm2g = "/html/body/div[2]/main/section/section/section/article/section[1]"
SECTION_CONTAINER_yf_1ce4p3e = "/html/body/div[2]/main/section/section/section/section/section"
UL_STREAM_ITEMS_yf_1drgw5l = "/html/body/div[2]/main/section/section/section/section/section/div/div/div/div/ul"
UL_STREAM_ITEMS_yf_9xydx9 = "/html/body/div[2]/div[3]/main/section/section/section/section/section/div/div[1]/div/div/ul"
UL_STREAM_ITEMS_yf_1qcp8cc = "/html/body/div[1]/div[4]/main/section/section/section/section/section/div/div/div/div/ul"
UL_STREAM_ITEMS_yf_ydpc1 = "/html/body/div[2]/div[3]/main/section/section/section/section/section/div/div/div[1]/div/ul"
UL_LIST_yf_12gc3ad = "/html/body/div[1]/div[4]/main/section/section/section/section/div/div/section[10]/section/div/div/div/div/ul"
A_PRIMARY_LINK_FIN_SIZE_SMALL_yf_1119g04z = "/html/body/div[1]/div[4]/main/section/section/section/section/section/div/div/div[2]/nav/div[2]/a"
# Robust: find ul by class "stream-items" (e.g. "stream-items yf-ydpc1") so it works when
# the news list is loaded by JS and absolute XPaths change.
UL_STREAM_ITEMS_BY_CLASS = "//ul[contains(@class, 'stream-items')]"