import requests
import json
import pandas as pd
import pickle


class YahooLinks:
    headers = {'content-type': 'application/json', 'cookie': 'AO=u=1; B=37ivp71boddqc&b=4&d=nVvahyJtYFi7ZtHyNuDJ&s=jg&i=o27ljC5vf.GapligXPer; A1=d=AQABBLirFF4CECJSD7_m11XFtM2ZUN_YJMwFEgABAgHyFGD_YOA9b2UB9iMAAAcITLeGVzj5yzMID6Nu5Ywub3_hmqZYoFz3qwkBBwoBnA&S=AQAAAuZ2CH-cMRaxSlWxZjLARFg; A3=d=AQABBLirFF4CECJSD7_m11XFtM2ZUN_YJMwFEgABAgHyFGD_YOA9b2UB9iMAAAcITLeGVzj5yzMID6Nu5Ywub3_hmqZYoFz3qwkBBwoBnA&S=AQAAAuZ2CH-cMRaxSlWxZjLARFg; GUC=AQABAgFgFPJg_0IdzgSP; cmp=v=18&t=1611907728&j=1&o=106; APID=UPb12a4ff6-6209-11eb-9e1a-0225c6d4cd42; thamba=2; EuConsent=CPAxG36PAxG36AOACBDEBACoAP_AAH_AACiQHCNd_X_fb39j-_59__t0eY1f9_7_v20zjgeds-8Nyd_X_L8X_2M7vB36pr4KuR4ku3bBAQFtHOncTQmx6IlVqTPsak2Mr7NKJ7PEinsbe2dYGHtfn9VT-ZKZr97s___7________79______3_vt_9__wOCAJMNS-AizEscCSaNKoUQIQriQ6AEAFFCMLRNYQErgp2VwEfoIGACA1ARgRAgxBRiyCAAAAAJKIgBADwQCIAiAQAAgBUgIQAEaAILACQMAgAFANCwAigCECQgyOCo5TAgIkWignkrAEou9jDCEMooAaAAA; A1S=d=AQABBLirFF4CECJSD7_m11XFtM2ZUN_YJMwFEgABAgHyFGD_YOA9b2UB9iMAAAcITLeGVzj5yzMID6Nu5Ywub3_hmqZYoFz3qwkBBwoBnA&S=AQAAAuZ2CH-cMRaxSlWxZjLARFg&j=GDPR; cmp=v=18&t=1616485448&j=1; PRF=t%3DTSLA%252BBA%252BGME'}
    url = 'https://finance.yahoo.com/_finance_doubledown/api/resource?bkt=JRVXP-finance_xgb_ctrl%2CFINAA023%2Cfinance-us-dweb-xray-v2chart-upsell-2%2Copenweb-finance-us-test2-nwtest%2Cfdw-brokercenter-5%2CFINAA031%2Cfdw-MAST2NativeBB-test1&crumb=up7MGaZi3m.&device=desktop&ecma=modern&feature=adsMigration%2CcanvassOffnet%2CccOnMute%2CdisableCommentsMessage%2Cdebouncesearch100%2CdeferDarla%2CecmaModern%2CemptyServiceWorker%2CenableCCPAFooter%2CenableCMP%2CenableConsentData%2CenableFeatureTours%2CenableFinancialsTemplate%2CenableFreeFinRichSearch%2CenableGuceJs%2CenableGuceJsOverlay%2CenableNavFeatureCue%2CenableNewResearchInsights%2CenablePfSummaryForEveryone%2CenablePremiumSingleCTA%2CenablePremiumUpsell%2CenablePrivacyUpdate%2CenableRebranding%2CenableStreamDebounce%2CenableTheming%2CenableUpgradeLeafPage%2CenableVideoURL%2CenableXrayNcp%2CenableXrayNcpInModal%2CenableXrayTickerEntities%2CenableYahooPlus%2CenableYahooSans%2CenableYodleeErrorMsgCriOS%2CncpListStream%2CncpPortfolioStream%2CncpQspStream%2CncpStream%2CncpStreamIntl%2CncpTopicStream%2CnewContentAttribution%2CnewLogo%2CoathPlayer%2CoptimizeSearch%2CrelatedVideoFeature%2CthreeAmigos%2CwaferHeader%2CuseNextGenHistory%2CvideoNativePlaylist%2CsunsetMotif2%2CenableUserPrefAPI%2Clivecoverage%2CdarlaFirstRenderingVisible%2CenableAdlite%2CenableTradeit%2CenableFeatureBar%2CenableSearchEnhancement%2CenableUserSentiment%2CenableBankrateWidget%2CenableYodlee%2CcanvassReplies%2CenablePremiumFinancials%2CenableInstapage%2CenableNewResearchFilterMW%2CshowExpiredIdeas%2CshowMorningStar%2CenableSEOResearchReport%2CenableSingleRail%2CenableUpgrade%2CenhanceAddToWL%2Carticle2_csn%2CsponsoredAds%2CenableStageAds%2CenableTradeItLinkBrokerSecondaryPromo%2CpremiumPromoHeader%2CenableQspPremiumPromoSmall%2CclientDelayNone%2CthreeAmigosMabEnabled%2CthreeAmigosAdsEnabledAndStreamIndex0%2CenableRelatedTickers%2CenableTasteMaker%2CenableNotification%2CfinanceRightRailA20%2CenableBrokerCenter&intl=us&lang=en-US&partner=none&prid=1fpq019g5j725&region=US&site=finance&tz=Europe%2FBerlin&ver=0.102.4538'
    tickers = pickle.load(open('tickers.p', 'rb'))

    def get_links(self):
        data = []
        for ticker in self.tickers:
            body = '{"requests":{"g0":{"resource":"StreamService","operation":"read","params":{"ui":{"comments_offnet":true,"editorial_featured_count":1,"image_quality_override":true,"link_out_allowed":true,"needtoknow_template":"filmstrip","ntk_bypassA3c":true,"pubtime_maxage":0,"relative_links":true,"show_comment_count":true,"smart_crop":true,"storyline_count":2,"storyline_enabled":true,"storyline_min":2,"summary":true,"thumbnail_size":100,"tiles":{"allowPartialRows":true,"doubleTallStart":0,"featured_label":false,"gradient":false,"height":175,"resizeImages":false,"textOnly":[{"backgroundColor":"#fff","foregroundColor":"#000"}],"width_max":300,"width_min":200},"view":"mega","editorial_content_count":6,"enable_lead_fallback_image":true,"finance_upsell_threshold":4},"forceJpg":true,"releasesParams":{"limit":20,"offset":0},"ncpParams":{"query":{"namespace":"finance","id":"tickers-news-stream","version":"v1","listAlias":"finance-US-en-US-ticker-news"}},"offnet":{"include_lcp":true,"use_preview":true,"url_scheme":"domain"},"useNCP":true,"video":{"enable_video_enrichment":true},"ads":{"ad_polices":true,"contentType":"video/mp4,application/x-shockwave-flash,application/vnd.apple.mpegurl","count":25,"enableFlashSale":true,"enableGeminiDealsWithoutBackground":true,"frequency":4,"geminiPromotionsEnabled":true,"generic_viewability":true,"inline_video":true,"partial_viewability":true,"pu":"finance.yahoo.com","se":5571994,"spaceid":95993639,"start_index":1,"timeout":0,"type":"STRM,STRM_CONTENT,STRM_VIDEO","useHqImg":true,"useResizedImages":true},"batches":{"pagination":true,"size":200,"timeout":1500,"total":170},"editors_picks":{"show_label":true},"enableAuthorBio":true,"max_exclude":0,"min_count":0,"min_count_error":true,"qsp_views":"news,video,ttext","service":{"specRetry":{"enabled":false}},"category":' + '"YFINANCE:{t}"'.format(t=ticker) + ',"pageContext":{"pageType":"utility","subscribed":"0","enablePremium":"0","eventName":"","topicName":"","category":"news","quoteType":"EQUITY","calendarType":"","screenerType":""},"content_type":"qsp","content_site":"finance"}}},"context":{"feature":"adsMigration,canvassOffnet,ccOnMute,disableCommentsMessage,debouncesearch100,deferDarla,ecmaModern,emptyServiceWorker,enableCCPAFooter,enableCMP,enableConsentData,enableFeatureTours,enableFinancialsTemplate,enableFreeFinRichSearch,enableGuceJs,enableGuceJsOverlay,enableNavFeatureCue,enableNewResearchInsights,enablePfSummaryForEveryone,enablePremiumSingleCTA,enablePremiumUpsell,enablePrivacyUpdate,enableRebranding,enableStreamDebounce,enableTheming,enableUpgradeLeafPage,enableVideoURL,enableXrayNcp,enableXrayNcpInModal,enableXrayTickerEntities,enableYahooPlus,enableYahooSans,enableYodleeErrorMsgCriOS,ncpListStream,ncpPortfolioStream,ncpQspStream,ncpStream,ncpStreamIntl,ncpTopicStream,newContentAttribution,newLogo,oathPlayer,optimizeSearch,relatedVideoFeature,threeAmigos,waferHeader,useNextGenHistory,videoNativePlaylist,sunsetMotif2,enableUserPrefAPI,livecoverage,darlaFirstRenderingVisible,enableAdlite,enableTradeit,enableFeatureBar,enableSearchEnhancement,enableUserSentiment,enableBankrateWidget,enableYodlee,canvassReplies,enablePremiumFinancials,enableInstapage,enableNewResearchFilterMW,showExpiredIdeas,showMorningStar,enableSEOResearchReport,enableSingleRail,enableUpgrade,enhanceAddToWL,article2_csn,sponsoredAds,enableStageAds,enableTradeItLinkBrokerSecondaryPromo,premiumPromoHeader,enableQspPremiumPromoSmall,clientDelayNone,threeAmigosMabEnabled,threeAmigosAdsEnabledAndStreamIndex0,enableRelatedTickers,enableTasteMaker,enableNotification,financeRightRailA20,enableBrokerCenter","bkt":["JRVXP-finance_xgb_ctrl","FINAA023","finance-us-dweb-xray-v2chart-upsell-2","openweb-finance-us-test2-nwtest","fdw-brokercenter-5","FINAA031","fdw-MAST2NativeBB-test1"],"crumb":"up7MGaZi3m.","device":"desktop","intl":"us","lang":"en-US","partner":"none","prid":"1fpq019g5j725","region":"US","site":"finance","tz":"Europe/Berlin","ver":"0.102.4538","ecma":"modern"}}'
            r = requests.post(url=self.url, headers=self.headers, data=body)
            json_data = json.loads(r.content)

            for item in json_data['g0']['data']['stream_items']:
                try:
                    features = {
                        'link': item['url'],
                        'publisher': item['publisher'],
                        'network': item['off_network'],
                        'stock': ticker
                    }

                    data.append(features)
                except:
                    print('No Scraping Possible')
                    pass
        return data

    def convert_csv(self):
        data = self.get_links()
        df = pd.DataFrame(data)
        mask = df.applymap(type) != bool
        d = {True: 'off_network', False: 'on_network'}
        df = df.where(mask, df.replace(d))
        df.to_csv('stock_requests3.csv')


if __name__ == '__main__':
    y = YahooLinks()
    y.convert_csv()


