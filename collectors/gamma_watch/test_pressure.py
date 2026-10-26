import unittest
from pressure import summarize
class PressureTests(unittest.TestCase):
 def test_full_window(self):
  r=summarize([dict(t=t,c=100+t/100) for t in range(0,901,5)],900)
  self.assertEqual([w['price_change'] for w in r['windows']],[.6,3.,9.])
  self.assertIsNone(r['signed_share_volume'])
 def test_warm_stale_gap(self):
  self.assertEqual(summarize([],900)['windows'][0]['status'],'warming_up')
  self.assertEqual(summarize([dict(t=840,c=100)],900)['windows'][0]['status'],'stale')
  self.assertEqual(summarize([dict(t=840,c=100),dict(t=900,c=101)],900)['windows'][0]['status'],'gap')
if __name__=='__main__':unittest.main()
