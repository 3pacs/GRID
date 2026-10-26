import unittest
from datetime import datetime, timezone
from sectors import summarize

class SectorTests(unittest.TestCase):
    def doc(self, expiry='260928', volume=0, bid=1, ask=1.1):
        return {'timestamp':'raw','data':{'symbol':'XLF','current_price':54,'prev_day_close':55,
          'options':[{'option':'XLF'+expiry+'C00054000','volume':volume,'open_interest':0,'bid':bid,'ask':ask}]}}
    def run_doc(self, doc):
        return summarize(doc,'XLF',datetime(2026,9,28,17,tzinfo=timezone.utc))
    def test_zero_is_known(self):
        r=self.run_doc(self.doc());self.assertEqual(r['today_volume'],0);self.assertTrue(r['has_today_expiry'])
    def test_missing_volume_unknown(self):
        self.assertIsNone(self.run_doc(self.doc(volume=None))['today_volume'])
    def test_friday_not_today(self):
        r=self.run_doc(self.doc('261002'));self.assertFalse(r['has_today_expiry']);self.assertIsNone(r['today_volume'])
    def test_expired_chain_unknown(self):
        self.assertIsNone(self.run_doc(self.doc('260925'))['has_today_expiry'])
    def test_bad_quotes_not_tight_spread(self):
        for b,a in [(0,.01),(2,1),(None,1)]:
            self.assertIsNone(self.run_doc(self.doc(bid=b,ask=a))['atm_spread_pct'])
    def test_symbol_mismatch(self):
        with self.assertRaises(ValueError): summarize(self.doc(),'XLK')
    def test_et_date_boundary(self):
        r=summarize(self.doc(),'XLF',datetime(2026,9,29,0,1,tzinfo=timezone.utc))
        self.assertTrue(r['has_today_expiry'])

if __name__=='__main__':unittest.main()
