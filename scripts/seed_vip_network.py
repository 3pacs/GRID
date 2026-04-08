#!/usr/bin/env python3
"""
seed_vip_network.py — Comprehensive VIP network seeder for GRID
Seeds 500+ billionaires, 200+ world leaders, 1000+ companies, 2000+ connections
Safe to re-run (upsert for actors, skip-on-conflict for connections)
"""

import sys
import os
import json
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db import get_engine
from sqlalchemy import text
from loguru import logger as log

NOW = datetime.now(timezone.utc).isoformat()

# ═══════════════════════════════════════════════════════════════════════════════
# EXISTING ACTOR IDS — do NOT duplicate these
# ═══════════════════════════════════════════════════════════════════════════════
EXISTING_IDS = {
    "fed_powell", "ecb_lagarde", "pboc_pan", "boj_ueda",
    "am_fink", "ind_musk", "ind_musk_expanded", "ind_bezos", "ind_bezos_expanded",
    "ind_buffett", "ind_zuckerberg", "ind_zuckerberg_expanded", "ind_jensen_expanded",
    "congress_pelosi", "congress_crenshaw", "royal_mbs", "royal_mbz", "treasury_yellen",
}


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1: BILLIONAIRES (Forbes 2025 top ~500)
# ═══════════════════════════════════════════════════════════════════════════════

def get_billionaires():
    """Forbes 2025 billionaires list — name, net worth (USD), country, source of wealth."""
    return [
        # Top 10
        ("bil_musk", "Elon Musk", 342000000000, "US", "Tesla, SpaceX", "sovereign"),
        ("bil_bezos", "Jeff Bezos", 233000000000, "US", "Amazon", "sovereign"),
        ("bil_zuckerberg", "Mark Zuckerberg", 206000000000, "US", "Meta", "sovereign"),
        ("bil_ellison", "Larry Ellison", 205000000000, "US", "Oracle", "sovereign"),
        ("bil_arnault", "Bernard Arnault", 190000000000, "France", "LVMH", "sovereign"),
        ("bil_gates", "Bill Gates", 157000000000, "US", "Microsoft", "sovereign"),
        ("bil_buffett", "Warren Buffett", 143000000000, "US", "Berkshire Hathaway", "sovereign"),
        ("bil_jensen_huang", "Jensen Huang", 127000000000, "US", "NVIDIA", "sovereign"),
        ("bil_walton_jim", "Jim Walton", 114000000000, "US", "Walmart", "sovereign"),
        ("bil_walton_rob", "Rob Walton", 113000000000, "US", "Walmart", "sovereign"),
        # 11–25
        ("bil_walton_alice", "Alice Walton", 112000000000, "US", "Walmart", "sovereign"),
        ("bil_ambani", "Mukesh Ambani", 109000000000, "India", "Reliance Industries", "sovereign"),
        ("bil_ballmer", "Steve Ballmer", 107000000000, "US", "Microsoft", "sovereign"),
        ("bil_page", "Larry Page", 105000000000, "US", "Google/Alphabet", "sovereign"),
        ("bil_brin", "Sergey Brin", 101000000000, "US", "Google/Alphabet", "sovereign"),
        ("bil_adani", "Gautam Adani", 86000000000, "India", "Adani Group", "sovereign"),
        ("bil_slim", "Carlos Slim", 85000000000, "Mexico", "Telecom", "sovereign"),
        ("bil_dell", "Michael Dell", 84000000000, "US", "Dell Technologies", "sovereign"),
        ("bil_koch_charles", "Charles Koch", 69000000000, "US", "Koch Industries", "sovereign"),
        ("bil_koch_julia", "Julia Koch", 68000000000, "US", "Koch Industries", "sovereign"),
        ("bil_bloomberg", "Michael Bloomberg", 67000000000, "US", "Bloomberg LP", "sovereign"),
        ("bil_adelson_miriam", "Miriam Adelson", 65000000000, "US", "Casinos", "sovereign"),
        ("bil_ma", "Jack Ma", 62000000000, "China", "Alibaba", "sovereign"),
        ("bil_plattner", "Hasso Plattner", 60000000000, "Germany", "SAP", "sovereign"),
        ("bil_ma_huateng", "Ma Huateng", 58000000000, "China", "Tencent", "sovereign"),
        # 26–50
        ("bil_pinault", "Francois Pinault", 55000000000, "France", "Kering", "sovereign"),
        ("bil_wertheimer_alain", "Alain Wertheimer", 54000000000, "France", "Chanel", "sovereign"),
        ("bil_wertheimer_gerard", "Gerard Wertheimer", 54000000000, "France", "Chanel", "sovereign"),
        ("bil_bettencourt", "Francoise Bettencourt Meyers", 53000000000, "France", "L'Oreal", "sovereign"),
        ("bil_agarwal", "Savitri Jindal", 48000000000, "India", "Steel", "sovereign"),
        ("bil_zhong", "Zhong Shanshan", 50000000000, "China", "Beverages, Pharma", "sovereign"),
        ("bil_naik_radha", "Radhakishan Damani", 35000000000, "India", "Retail", "regional"),
        ("bil_hartono_robert", "Robert Budi Hartono", 26000000000, "Indonesia", "Banking, Tobacco", "regional"),
        ("bil_hartono_michael", "Michael Hartono", 25500000000, "Indonesia", "Banking, Tobacco", "regional"),
        ("bil_thomson_david", "David Thomson", 63000000000, "Canada", "Thomson Reuters", "sovereign"),
        ("bil_weston_galen", "Galen Weston", 14000000000, "Canada", "Retail", "regional"),
        ("bil_dimon", "Jamie Dimon", 2800000000, "US", "JPMorgan Chase", "institutional"),
        ("bil_dalio", "Ray Dalio", 26000000000, "US", "Bridgewater Associates", "regional"),
        ("bil_simons_jim", "Jim Simons", 31000000000, "US", "Renaissance Technologies", "regional"),
        ("bil_griffin", "Ken Griffin", 43000000000, "US", "Citadel", "sovereign"),
        ("bil_cohen_steve", "Steve Cohen", 21000000000, "US", "Point72", "regional"),
        ("bil_ackman", "Bill Ackman", 9000000000, "US", "Pershing Square", "institutional"),
        ("bil_druckenmiller", "Stanley Druckenmiller", 11000000000, "US", "Duquesne", "institutional"),
        ("bil_icahn", "Carl Icahn", 9000000000, "US", "Icahn Enterprises", "institutional"),
        ("bil_soros", "George Soros", 7000000000, "US", "Investments", "institutional"),
        ("bil_schwarzman", "Stephen Schwarzman", 42000000000, "US", "Blackstone", "sovereign"),
        ("bil_kravis", "Henry Kravis", 13000000000, "US", "KKR", "regional"),
        ("bil_rubenstein", "David Rubenstein", 5000000000, "US", "Carlyle Group", "institutional"),
        ("bil_singer", "Paul Singer", 6000000000, "US", "Elliott Management", "institutional"),
        ("bil_loeb", "Daniel Loeb", 4000000000, "US", "Third Point", "institutional"),
        # 51–100
        ("bil_zhangyiming", "Zhang Yiming", 49000000000, "China", "ByteDance/TikTok", "sovereign"),
        ("bil_li_ka_shing", "Li Ka-shing", 38000000000, "Hong Kong", "CK Hutchison", "sovereign"),
        ("bil_lee_shau_kee", "Lee Shau Kee", 28000000000, "Hong Kong", "Real Estate", "regional"),
        ("bil_pony_ma", "Pony Ma (Ma Huateng)", 48000000000, "China", "Tencent", "sovereign"),
        ("bil_ren_zhengfei", "Ren Zhengfei", 2000000000, "China", "Huawei", "institutional"),
        ("bil_lei_jun", "Lei Jun", 22000000000, "China", "Xiaomi", "regional"),
        ("bil_colin_huang", "Colin Huang", 36000000000, "China", "PDD/Pinduoduo", "sovereign"),
        ("bil_william_ding", "William Ding", 23000000000, "China", "NetEase", "regional"),
        ("bil_he_xiangjian", "He Xiangjian", 25000000000, "China", "Midea", "regional"),
        ("bil_li_shufu", "Li Shufu", 19000000000, "China", "Geely", "regional"),
        ("bil_wang_chuanfu", "Wang Chuanfu", 18000000000, "China", "BYD", "regional"),
        ("bil_robin_li", "Robin Li", 13000000000, "China", "Baidu", "regional"),
        ("bil_liu_qiangdong", "Liu Qiangdong", 17000000000, "China", "JD.com", "regional"),
        ("bil_altman", "Sam Altman", 2000000000, "US", "OpenAI", "institutional"),
        ("bil_thiel", "Peter Thiel", 14000000000, "US", "Founders Fund, Palantir", "regional"),
        ("bil_dorsey", "Jack Dorsey", 5000000000, "US", "Block/Twitter", "institutional"),
        ("bil_hastings", "Reed Hastings", 6000000000, "US", "Netflix", "institutional"),
        ("bil_nadella", "Satya Nadella", 1200000000, "US", "Microsoft", "institutional"),
        ("bil_cook", "Tim Cook", 2300000000, "US", "Apple", "institutional"),
        ("bil_pichai", "Sundar Pichai", 1500000000, "US", "Google/Alphabet", "institutional"),
        ("bil_jassy", "Andy Jassy", 800000000, "US", "Amazon", "institutional"),
        ("bil_diess", "Herbert Diess", 500000000, "Germany", "Volkswagen", "institutional"),
        ("bil_son", "Masayoshi Son", 22000000000, "Japan", "SoftBank", "regional"),
        ("bil_tadashi_yanai", "Tadashi Yanai", 39000000000, "Japan", "Uniqlo/Fast Retailing", "sovereign"),
        ("bil_takemitsu_takizaki", "Takemitsu Takizaki", 34000000000, "Japan", "Keyence", "sovereign"),
        ("bil_shiv_nadar", "Shiv Nadar", 32000000000, "India", "HCL Technologies", "regional"),
        ("bil_azim_premji", "Azim Premji", 31000000000, "India", "Wipro", "regional"),
        ("bil_kumar_birla", "Kumar Mangalam Birla", 21000000000, "India", "Aditya Birla Group", "regional"),
        ("bil_gopichand_hinduja", "Gopichand Hinduja", 20000000000, "India", "Hinduja Group", "regional"),
        ("bil_uday_kotak", "Uday Kotak", 17000000000, "India", "Kotak Mahindra", "regional"),
        ("bil_cyrus_poonawalla", "Cyrus Poonawalla", 26000000000, "India", "Serum Institute", "regional"),
        ("bil_dilip_shanghvi", "Dilip Shanghvi", 27000000000, "India", "Sun Pharma", "regional"),
        ("bil_alwaleed", "Prince Alwaleed bin Talal", 18000000000, "Saudi Arabia", "Kingdom Holding", "regional"),
        ("bil_al_rajhi", "Sulaiman Al Rajhi", 14000000000, "Saudi Arabia", "Banking", "regional"),
        ("bil_batista_safra", "Joseph Safra Estate", 22000000000, "Brazil", "Banking", "regional"),
        ("bil_jorge_lemann", "Jorge Paulo Lemann", 14000000000, "Brazil", "3G Capital, AB InBev", "regional"),
        ("bil_eduardo_saverin", "Eduardo Saverin", 28000000000, "Brazil", "Facebook/Meta", "regional"),
        ("bil_marcel_herrmann", "Marcel Herrmann Telles", 8000000000, "Brazil", "3G Capital", "institutional"),
        ("bil_irving_family", "Arthur Irving", 7000000000, "Canada", "Irving Oil", "institutional"),
        ("bil_saputo_lino", "Lino Saputo", 7000000000, "Canada", "Saputo Inc", "institutional"),
        ("bil_perez_family", "German Larrea Mota Velasco", 32000000000, "Mexico", "Mining", "regional"),
        ("bil_antonio_del_valle", "Antonio del Valle", 12000000000, "Mexico", "Chemicals, Banking", "regional"),
        ("bil_ricardo_salinas", "Ricardo Salinas Pliego", 13000000000, "Mexico", "TV Azteca, Elektra", "regional"),
        ("bil_luis_carlos_sarmiento", "Luis Carlos Sarmiento", 10000000000, "Colombia", "Banking", "institutional"),
        ("bil_iris_fontbona", "Iris Fontbona", 26000000000, "Chile", "Mining", "regional"),
        ("bil_horst_paulmann", "Horst Paulmann", 4000000000, "Chile", "Retail", "institutional"),
        ("bil_stef_wertheimer", "Stef Wertheimer", 7000000000, "Israel", "Manufacturing", "institutional"),
        ("bil_eyal_ofer", "Eyal Ofer", 23000000000, "Israel", "Shipping, Real Estate", "regional"),
        ("bil_idan_ofer", "Idan Ofer", 14000000000, "Israel", "Shipping, Energy", "regional"),
        # 100–200
        ("bil_dietrich_mateschitz_estate", "Mark Mateschitz", 37000000000, "Austria", "Red Bull", "sovereign"),
        ("bil_reinhold_wuerth", "Reinhold Wuerth", 24000000000, "Germany", "Wuerth Group", "regional"),
        ("bil_dieter_schwarz", "Dieter Schwarz", 40000000000, "Germany", "Lidl, Kaufland", "sovereign"),
        ("bil_klaus_michael_kuehne", "Klaus-Michael Kuehne", 36000000000, "Germany", "Kuehne+Nagel", "sovereign"),
        ("bil_stefan_quandt", "Stefan Quandt", 23000000000, "Germany", "BMW", "regional"),
        ("bil_susanne_klatten", "Susanne Klatten", 21000000000, "Germany", "BMW, Altana", "regional"),
        ("bil_karl_albrecht_jr", "Karl Albrecht Jr", 22000000000, "Germany", "Aldi", "regional"),
        ("bil_theo_albrecht_jr", "Theo Albrecht Jr", 19000000000, "Germany", "Aldi Nord, Trader Joe's", "regional"),
        ("bil_beate_heister", "Beate Heister", 20000000000, "Germany", "Aldi Sued", "regional"),
        ("bil_giovanni_ferrero", "Giovanni Ferrero", 42000000000, "Italy", "Ferrero/Nutella", "sovereign"),
        ("bil_silvio_berlusconi_heirs", "Marina Berlusconi", 7000000000, "Italy", "Fininvest, Mediaset", "institutional"),
        ("bil_giorgio_armani", "Giorgio Armani", 12000000000, "Italy", "Fashion", "regional"),
        ("bil_miuccia_prada", "Miuccia Prada", 7000000000, "Italy", "Prada", "institutional"),
        ("bil_leonard_lauder", "Leonard Lauder", 26000000000, "US", "Estee Lauder", "regional"),
        ("bil_ronald_lauder", "Ronald Lauder", 5000000000, "US", "Estee Lauder", "institutional"),
        ("bil_rupert_murdoch", "Rupert Murdoch", 21000000000, "US", "News Corp, Fox", "regional"),
        ("bil_donald_trump", "Donald Trump", 6000000000, "US", "Real Estate, Media", "institutional"),
        ("bil_charles_ergen", "Charles Ergen", 8000000000, "US", "Dish Network", "institutional"),
        ("bil_john_mars", "John Mars", 42000000000, "US", "Mars Inc", "sovereign"),
        ("bil_jacqueline_mars", "Jacqueline Mars", 42000000000, "US", "Mars Inc", "sovereign"),
        ("bil_phil_knight", "Phil Knight", 47000000000, "US", "Nike", "sovereign"),
        ("bil_mark_knight", "Mark Parker", 2000000000, "US", "Nike", "institutional"),
        ("bil_jim_kennedy", "Jim Kennedy", 11000000000, "US", "Cox Enterprises", "institutional"),
        ("bil_donald_newhouse", "Donald Newhouse", 15000000000, "US", "Advance Publications", "regional"),
        ("bil_leonard_stern", "Leonard Stern", 6000000000, "US", "Hartz Group", "institutional"),
        ("bil_lukas_walton", "Lukas Walton", 37000000000, "US", "Walmart", "sovereign"),
        ("bil_abigail_johnson", "Abigail Johnson", 31000000000, "US", "Fidelity", "regional"),
        ("bil_charles_schwab", "Charles Schwab", 14000000000, "US", "Charles Schwab Corp", "regional"),
        ("bil_steve_wynn", "Steve Wynn", 3000000000, "US", "Casinos", "institutional"),
        ("bil_thomas_peterffy", "Thomas Peterffy", 28000000000, "US", "Interactive Brokers", "regional"),
        ("bil_robert_kraft", "Robert Kraft", 12000000000, "US", "New England Patriots, Kraft Group", "regional"),
        ("bil_jerry_jones", "Jerry Jones", 14000000000, "US", "Dallas Cowboys", "regional"),
        ("bil_stan_kroenke", "Stan Kroenke", 15000000000, "US", "Sports, Real Estate", "regional"),
        ("bil_steve_cohen2", "Steven Cohen", 21000000000, "US", "Point72, NY Mets", "regional"),
        ("bil_mark_cuban", "Mark Cuban", 6000000000, "US", "Investments", "institutional"),
        ("bil_john_paulson", "John Paulson", 5000000000, "US", "Paulson & Co", "institutional"),
        ("bil_james_simons", "James Simons Estate", 31000000000, "US", "Renaissance Technologies", "regional"),
        ("bil_david_tepper", "David Tepper", 20000000000, "US", "Appaloosa Management", "regional"),
        ("bil_bruce_kovner", "Bruce Kovner", 7000000000, "US", "Caxton Associates", "institutional"),
        ("bil_chase_coleman", "Chase Coleman", 8000000000, "US", "Tiger Global", "institutional"),
        ("bil_israel_englander", "Israel Englander", 12000000000, "US", "Millennium Management", "regional"),
        ("bil_david_shaw", "David Shaw", 9000000000, "US", "D.E. Shaw", "institutional"),
        ("bil_jim_levin", "Jim Levin", 2500000000, "US", "Levin Capital", "institutional"),
        ("bil_howard_marks", "Howard Marks", 2500000000, "US", "Oaktree Capital", "institutional"),
        ("bil_leon_black", "Leon Black", 10000000000, "US", "Apollo Global", "institutional"),
        ("bil_marc_rowan", "Marc Rowan", 9000000000, "US", "Apollo Global", "institutional"),
        ("bil_josh_harris", "Josh Harris", 8000000000, "US", "Apollo Global", "institutional"),
        ("bil_orlando_bravo", "Orlando Bravo", 8000000000, "US", "Thoma Bravo", "institutional"),
        ("bil_robert_smith", "Robert F. Smith", 9000000000, "US", "Vista Equity", "institutional"),
        ("bil_barry_sternlicht", "Barry Sternlicht", 4000000000, "US", "Starwood Capital", "institutional"),
        # 200–300
        ("bil_li_hejun", "Li Hejun", 2000000000, "China", "Hanergy", "institutional"),
        ("bil_liu_yonghao", "Liu Yonghao", 8000000000, "China", "New Hope Group", "institutional"),
        ("bil_guo_guangchang", "Guo Guangchang", 6000000000, "China", "Fosun", "institutional"),
        ("bil_wang_jianlin", "Wang Jianlin", 10000000000, "China", "Wanda Group", "institutional"),
        ("bil_yang_huiyan", "Yang Huiyan", 7000000000, "China", "Country Garden", "institutional"),
        ("bil_hui_ka_yan", "Hui Ka Yan", 2000000000, "China", "Evergrande", "institutional"),
        ("bil_lu_yongxiang", "Lu Yongxiang", 3000000000, "China", "Haier", "institutional"),
        ("bil_li_xiting", "Li Xiting", 14000000000, "China", "Mindray", "regional"),
        ("bil_qin_yinglin", "Qin Yinglin", 10000000000, "China", "Muyuan Foods", "institutional"),
        ("bil_sun_piaoyang", "Sun Piaoyang", 15000000000, "China", "Hengrui Medicine", "regional"),
        ("bil_alisher_usmanov", "Alisher Usmanov", 16000000000, "Russia", "Mining, Telecom", "regional"),
        ("bil_vladimir_potanin", "Vladimir Potanin", 24000000000, "Russia", "Norilsk Nickel", "regional"),
        ("bil_vladimir_lisin", "Vladimir Lisin", 26000000000, "Russia", "NLMK", "regional"),
        ("bil_vagit_alekperov", "Vagit Alekperov", 18000000000, "Russia", "Lukoil", "regional"),
        ("bil_leonid_mikhelson", "Leonid Mikhelson", 20000000000, "Russia", "Novatek", "regional"),
        ("bil_gennady_timchenko", "Gennady Timchenko", 14000000000, "Russia", "Volga Group", "regional"),
        ("bil_mikhail_fridman", "Mikhail Fridman", 11000000000, "Russia", "Alfa Group", "institutional"),
        ("bil_roman_abramovich", "Roman Abramovich", 10000000000, "Russia", "Steel, Investments", "institutional"),
        ("bil_alexey_mordashov", "Alexey Mordashov", 18000000000, "Russia", "Severstal", "regional"),
        ("bil_andrey_melnichenko", "Andrey Melnichenko", 17000000000, "Russia", "SUEK, EuroChem", "regional"),
        ("bil_lakshmi_mittal", "Lakshmi Mittal", 18000000000, "India", "ArcelorMittal", "regional"),
        ("bil_ratan_tata_estate", "Ratan Tata Estate", 1000000000, "India", "Tata Group", "institutional"),
        ("bil_noel_tata", "Noel Tata", 1000000000, "India", "Tata Group", "institutional"),
        ("bil_pallonji_mistry_estate", "Pallonji Mistry Estate", 20000000000, "India", "Shapoorji Pallonji", "regional"),
        ("bil_anand_mahindra", "Anand Mahindra", 3000000000, "India", "Mahindra Group", "institutional"),
        ("bil_sunil_mittal", "Sunil Bharti Mittal", 19000000000, "India", "Bharti Airtel", "regional"),
        ("bil_kumar_birla2", "Kumar Mangalam Birla", 21000000000, "India", "Aditya Birla", "regional"),
        ("bil_falguni_nayar", "Falguni Nayar", 7000000000, "India", "Nykaa", "institutional"),
        ("bil_nikhil_kamath", "Nikhil Kamath", 5000000000, "India", "Zerodha", "institutional"),
        ("bil_nithin_kamath", "Nithin Kamath", 5000000000, "India", "Zerodha", "institutional"),
        ("bil_nassef_sawiris", "Nassef Sawiris", 10000000000, "Egypt", "OCI, Adidas", "institutional"),
        ("bil_naguib_sawiris", "Naguib Sawiris", 3000000000, "Egypt", "Orascom", "institutional"),
        ("bil_aliko_dangote", "Aliko Dangote", 28000000000, "Nigeria", "Dangote Group", "regional"),
        ("bil_mike_adenuga", "Mike Adenuga", 7000000000, "Nigeria", "Telecom, Oil", "institutional"),
        ("bil_nicky_oppenheimer", "Nicky Oppenheimer", 10000000000, "South Africa", "De Beers, Diamonds", "institutional"),
        ("bil_johann_rupert", "Johann Rupert", 11000000000, "South Africa", "Richemont", "institutional"),
        ("bil_patrice_motsepe", "Patrice Motsepe", 3000000000, "South Africa", "Mining", "institutional"),
        ("bil_tony_elumelu", "Tony Elumelu", 1000000000, "Nigeria", "Banking", "institutional"),
        ("bil_abdulsamad_rabiu", "Abdulsamad Rabiu", 8000000000, "Nigeria", "BUA Group", "institutional"),
        ("bil_strive_masiyiwa", "Strive Masiyiwa", 2000000000, "Zimbabwe", "Econet", "institutional"),
        ("bil_mohammed_al_amoudi", "Mohammed Al Amoudi", 7000000000, "Ethiopia", "MIDROC", "institutional"),
        ("bil_james_dyson", "James Dyson", 23000000000, "UK", "Dyson", "regional"),
        ("bil_galen_weston_jr", "Galen Weston Jr", 14000000000, "Canada", "Loblaw", "regional"),
        ("bil_jim_ratcliffe", "Jim Ratcliffe", 20000000000, "UK", "Ineos", "regional"),
        ("bil_hinduja_family", "Hinduja Family", 37000000000, "UK", "Hinduja Group", "sovereign"),
        ("bil_len_blavatnik", "Len Blavatnik", 33000000000, "UK", "Access Industries", "regional"),
        ("bil_james_wiltshire", "Charlene de Carvalho-Heineken", 16000000000, "Netherlands", "Heineken", "regional"),
        ("bil_stefan_persson", "Stefan Persson", 19000000000, "Sweden", "H&M", "regional"),
        ("bil_hans_rausing", "Hans Rausing", 14000000000, "Sweden", "Tetra Laval", "regional"),
        ("bil_ernesto_bertarelli", "Ernesto Bertarelli", 10000000000, "Switzerland", "Biotech", "institutional"),
        ("bil_jorge_moll", "Jorge Moll Filho", 12000000000, "Brazil", "Healthcare", "regional"),
        ("bil_marcel_telles", "Marcel Herrmann Telles", 8000000000, "Brazil", "3G Capital", "institutional"),
        # 300–400
        ("bil_amancio_ortega", "Amancio Ortega", 86000000000, "Spain", "Zara/Inditex", "sovereign"),
        ("bil_sandra_ortega", "Sandra Ortega Mera", 8000000000, "Spain", "Inditex", "institutional"),
        ("bil_rafael_del_pino", "Rafael del Pino", 6000000000, "Spain", "Ferrovial", "institutional"),
        ("bil_mswati_iii", "King Mswati III", 5000000000, "Eswatini", "Monarchy", "institutional"),
        ("bil_hassanal_bolkiah", "Sultan of Brunei", 30000000000, "Brunei", "Oil, Investments", "regional"),
        ("bil_bhumibol_estate", "Thai Crown Property Bureau", 30000000000, "Thailand", "Crown Property", "regional"),
        ("bil_dhanin_chearavanont", "Dhanin Chearavanont", 20000000000, "Thailand", "CP Group", "regional"),
        ("bil_charoen_sirivadhanabhakdi", "Charoen Sirivadhanabhakdi", 14000000000, "Thailand", "ThaiBev", "regional"),
        ("bil_robert_kuok", "Robert Kuok", 12000000000, "Malaysia", "Kuok Group", "regional"),
        ("bil_quek_leng_chan", "Quek Leng Chan", 10000000000, "Malaysia", "Hong Leong", "institutional"),
        ("bil_antonio_sy_jr", "Henry Sy Jr", 5000000000, "Philippines", "SM Investments", "institutional"),
        ("bil_enrique_razon", "Enrique Razon Jr", 8000000000, "Philippines", "Ports, Casinos", "institutional"),
        ("bil_manny_villar", "Manny Villar", 8000000000, "Philippines", "Real Estate", "institutional"),
        ("bil_pham_nhat_vuong", "Pham Nhat Vuong", 9000000000, "Vietnam", "Vingroup", "institutional"),
        ("bil_nguyen_thi_phuong_thao", "Nguyen Thi Phuong Thao", 3000000000, "Vietnam", "VietJet", "institutional"),
        ("bil_r_budi_gunawan", "Anthoni Salim", 7000000000, "Indonesia", "Salim Group", "institutional"),
        ("bil_sri_prakash_lohia", "Sri Prakash Lohia", 6000000000, "Indonesia", "Indorama", "institutional"),
        ("bil_low_tuck_kwong", "Low Tuck Kwong", 6000000000, "Indonesia", "Coal Mining", "institutional"),
        ("bil_tsai_eng_meng", "Tsai Eng-Meng", 8000000000, "Taiwan", "Want Want", "institutional"),
        ("bil_terry_gou", "Terry Gou", 7000000000, "Taiwan", "Foxconn", "institutional"),
        ("bil_lin_yu_lin", "Daniel Tsai", 6000000000, "Taiwan", "Fubon Group", "institutional"),
        ("bil_kwon_hyuk_bin", "Kim Beom-su", 7000000000, "South Korea", "Kakao", "institutional"),
        ("bil_jay_y_lee", "Jay Y. Lee", 12000000000, "South Korea", "Samsung", "regional"),
        ("bil_chung_euisun", "Chung Euisun", 10000000000, "South Korea", "Hyundai Motor", "institutional"),
        ("bil_seo_jung_jin", "Seo Jung-Jin", 8000000000, "South Korea", "Celltrion", "institutional"),
        ("bil_lee_jae_yong", "Lee Jae-yong", 12000000000, "South Korea", "Samsung", "regional"),
        ("bil_chey_tae_won", "Chey Tae-won", 5000000000, "South Korea", "SK Group", "institutional"),
        ("bil_masayoshi_son2", "Masayoshi Son", 22000000000, "Japan", "SoftBank", "regional"),
        ("bil_nobutada_saji", "Nobutada Saji", 8000000000, "Japan", "Suntory", "institutional"),
        ("bil_yusaku_maezawa", "Yusaku Maezawa", 2000000000, "Japan", "Zozo", "institutional"),
        ("bil_hiroshi_mikitani", "Hiroshi Mikitani", 5000000000, "Japan", "Rakuten", "institutional"),
        # 400–500+
        ("bil_mike_cannon_brookes", "Mike Cannon-Brookes", 15000000000, "Australia", "Atlassian", "regional"),
        ("bil_scott_farquhar", "Scott Farquhar", 14000000000, "Australia", "Atlassian", "regional"),
        ("bil_gina_rinehart", "Gina Rinehart", 30000000000, "Australia", "Mining", "regional"),
        ("bil_andrew_forrest", "Andrew Forrest", 25000000000, "Australia", "Fortescue Metals", "regional"),
        ("bil_harry_triguboff", "Harry Triguboff", 15000000000, "Australia", "Meriton", "regional"),
        ("bil_frank_lowy", "Frank Lowy", 7000000000, "Australia", "Westfield", "institutional"),
        ("bil_graeme_hart", "Graeme Hart", 10000000000, "New Zealand", "Rank Group", "institutional"),
        ("bil_richard_branson", "Richard Branson", 3000000000, "UK", "Virgin Group", "institutional"),
        ("bil_mike_ashley", "Mike Ashley", 4000000000, "UK", "Frasers Group", "institutional"),
        ("bil_denise_coates", "Denise Coates", 7000000000, "UK", "Bet365", "institutional"),
        ("bil_vakil_ziyad", "Ziyad Manasir", 3000000000, "Russia", "Construction", "institutional"),
        ("bil_robert_mercer", "Robert Mercer", 2000000000, "US", "Renaissance Technologies", "institutional"),
        ("bil_peter_thiel2", "Peter Thiel", 14000000000, "US", "Palantir, Founders Fund", "regional"),
        ("bil_palmer_luckey", "Palmer Luckey", 10000000000, "US", "Anduril", "institutional"),
        ("bil_brian_chesky", "Brian Chesky", 12000000000, "US", "Airbnb", "regional"),
        ("bil_travis_kalanick", "Travis Kalanick", 3000000000, "US", "Uber", "institutional"),
        ("bil_garrett_camp", "Garrett Camp", 3000000000, "US", "Uber", "institutional"),
        ("bil_evan_spiegel", "Evan Spiegel", 10000000000, "US", "Snap Inc", "institutional"),
        ("bil_bobby_murphy", "Bobby Murphy", 9000000000, "US", "Snap Inc", "institutional"),
        ("bil_dustin_moskovitz", "Dustin Moskovitz", 18000000000, "US", "Facebook/Asana", "regional"),
        ("bil_eric_schmidt", "Eric Schmidt", 24000000000, "US", "Google/Alphabet", "regional"),
        ("bil_john_doerr", "John Doerr", 11000000000, "US", "Kleiner Perkins", "institutional"),
        ("bil_reid_hoffman", "Reid Hoffman", 2500000000, "US", "LinkedIn", "institutional"),
        ("bil_marc_andreessen", "Marc Andreessen", 2000000000, "US", "a16z", "institutional"),
        ("bil_vinod_khosla", "Vinod Khosla", 6000000000, "US", "Khosla Ventures", "institutional"),
        ("bil_patrick_collison", "Patrick Collison", 12000000000, "US", "Stripe", "regional"),
        ("bil_john_collison", "John Collison", 12000000000, "US", "Stripe", "regional"),
        ("bil_brian_armstrong", "Brian Armstrong", 11000000000, "US", "Coinbase", "institutional"),
        ("bil_changpeng_zhao", "Changpeng Zhao", 33000000000, "US", "Binance", "regional"),
        ("bil_michael_saylor", "Michael Saylor", 7000000000, "US", "MicroStrategy", "institutional"),
        ("bil_barry_diller", "Barry Diller", 5000000000, "US", "IAC, Expedia", "institutional"),
        ("bil_david_geffen", "David Geffen", 10000000000, "US", "DreamWorks, Music", "institutional"),
        ("bil_oprah_winfrey", "Oprah Winfrey", 3000000000, "US", "Media", "institutional"),
        ("bil_jay_z", "Jay-Z", 2500000000, "US", "Music, Investments", "institutional"),
        ("bil_rihanna", "Rihanna", 1400000000, "Barbados", "Fenty, Music", "institutional"),
        ("bil_tyler_perry", "Tyler Perry", 1000000000, "US", "Entertainment", "institutional"),
        ("bil_lebron_james", "LeBron James", 1200000000, "US", "Basketball, Investments", "institutional"),
        ("bil_steven_spielberg", "Steven Spielberg", 4000000000, "US", "Film", "institutional"),
        ("bil_george_lucas", "George Lucas", 5000000000, "US", "Lucasfilm", "institutional"),
        ("bil_kanye_west", "Kanye West", 2000000000, "US", "Music, Fashion", "institutional"),
        ("bil_michael_jordan", "Michael Jordan", 3000000000, "US", "Basketball, Nike", "institutional"),
        ("bil_kwong_siu_hing", "Kwong Siu-hing", 15000000000, "Hong Kong", "Sun Hung Kai Properties", "regional"),
        ("bil_peter_woo", "Peter Woo", 13000000000, "Hong Kong", "Wharf Holdings", "regional"),
        ("bil_pansy_ho", "Pansy Ho", 5000000000, "Hong Kong", "MGM China, Shun Tak", "institutional"),
        ("bil_cheng_yu_tung_family", "Henry Cheng", 20000000000, "Hong Kong", "Chow Tai Fook", "regional"),
        ("bil_lee_man_tat", "Lee Man Tat", 17000000000, "Hong Kong", "Lee Kum Kee", "regional"),
        ("bil_mikhail_prokhorov", "Mikhail Prokhorov", 10000000000, "Russia", "Investments", "institutional"),
        ("bil_petr_kellner_estate", "Renata Kellnerova", 18000000000, "Czech Republic", "PPF Group", "regional"),
        ("bil_daniel_kretinsky", "Daniel Kretinsky", 10000000000, "Czech Republic", "Energy, Media", "institutional"),
        ("bil_andrej_babis", "Andrej Babis", 4000000000, "Czech Republic", "Agrofert", "institutional"),
        ("bil_hans_peter_wild", "Hans Peter Wild", 6000000000, "Germany", "Capri-Sun", "institutional"),
        ("bil_merckle_ludwig", "Ludwig Merckle", 8000000000, "Germany", "Ratiopharm", "institutional"),
        ("bil_erivan_haub", "Christian Haub", 7000000000, "Germany", "Tengelmann", "institutional"),
        ("bil_xavier_niel", "Xavier Niel", 9000000000, "France", "Free, Iliad", "institutional"),
        ("bil_patrick_drahi", "Patrick Drahi", 5000000000, "France", "Altice", "institutional"),
        ("bil_dassault_family", "Dassault Family", 28000000000, "France", "Dassault Group", "regional"),
        ("bil_hermes_family", "Hermes Family", 90000000000, "France", "Hermes", "sovereign"),
        ("bil_rodolphe_saade", "Rodolphe Saade", 17000000000, "France", "CMA CGM", "regional"),
        ("bil_goh_cheng_liang", "Goh Cheng Liang", 18000000000, "Singapore", "Nippon Paint", "regional"),
        ("bil_eduardo_saverin2", "Eduardo Saverin", 28000000000, "Singapore", "B Capital", "regional"),
        ("bil_kwek_leng_beng", "Kwek Leng Beng", 8000000000, "Singapore", "CDL", "institutional"),
        ("bil_peter_lim", "Peter Lim", 3000000000, "Singapore", "Investments", "institutional"),
        ("bil_forrest_li", "Forrest Li", 2000000000, "Singapore", "Sea Limited", "institutional"),
        ("bil_wang_xing", "Wang Xing", 8000000000, "China", "Meituan", "institutional"),
        ("bil_huang_zheng", "Huang Zheng (Colin Huang)", 36000000000, "China", "Pinduoduo/PDD", "sovereign"),
        ("bil_zhang_zhidong", "Zhang Zhidong", 5000000000, "China", "Tencent co-founder", "institutional"),
        ("bil_zeng_yuqun", "Zeng Yuqun", 35000000000, "China", "CATL", "sovereign"),
        ("bil_zong_qinghou_estate", "Zong Fuli", 5000000000, "China", "Wahaha", "institutional"),
        ("bil_pang_kang", "Pang Kang", 9000000000, "China", "Foshan Haitian", "institutional"),
        ("bil_liang_wengen", "Liang Wengen", 6000000000, "China", "Sany Group", "institutional"),
        ("bil_frank_stronach", "Frank Stronach", 2000000000, "Canada", "Magna International", "institutional"),
        ("bil_david_cheriton", "David Cheriton", 10000000000, "US", "Venture Capital", "institutional"),
        ("bil_john_menard", "John Menard Jr", 16000000000, "US", "Menards", "regional"),
        ("bil_harold_hamm", "Harold Hamm", 18000000000, "US", "Continental Resources", "regional"),
        ("bil_ty_warner", "Ty Warner", 5000000000, "US", "Beanie Babies", "institutional"),
        ("bil_patrick_soon_shiong", "Patrick Soon-Shiong", 7000000000, "US", "Biotech, LA Times", "institutional"),
        ("bil_ernest_garcia_iii", "Ernest Garcia III", 5000000000, "US", "Carvana", "institutional"),
        ("bil_ernest_garcia_ii", "Ernest Garcia II", 17000000000, "US", "DriveTime, Carvana", "regional"),
        ("bil_iovance", "Stephen Ross", 12000000000, "US", "Related Companies", "regional"),
        ("bil_sam_bankman_fried_estate", "Sam Bankman-Fried", 0, "US", "FTX (collapsed)", "individual"),
        ("bil_jared_isaacman", "Jared Isaacman", 2000000000, "US", "Shift4 Payments", "institutional"),
        ("bil_joe_gebbia", "Joe Gebbia", 4000000000, "US", "Airbnb", "institutional"),
        ("bil_nathan_blecharczyk", "Nathan Blecharczyk", 5000000000, "US", "Airbnb", "institutional"),
        ("bil_drew_houston", "Drew Houston", 3000000000, "US", "Dropbox", "institutional"),
        ("bil_daniel_ek", "Daniel Ek", 5000000000, "Sweden", "Spotify", "institutional"),
        ("bil_martin_lorentzon", "Martin Lorentzon", 4000000000, "Sweden", "Spotify", "institutional"),
        ("bil_mark_mateschitz", "Mark Mateschitz", 37000000000, "Austria", "Red Bull", "sovereign"),
        # ── Additional billionaires 320–530+ ──
        ("bil_ray_dalio2", "Ray Dalio", 26000000000, "US", "Bridgewater Associates", "regional"),
        ("bil_ken_langone", "Ken Langone", 7000000000, "US", "Home Depot co-founder", "institutional"),
        ("bil_herbert_kohler", "Herbert Kohler Jr Estate", 8000000000, "US", "Kohler Co", "institutional"),
        ("bil_james_goodnight", "James Goodnight", 8000000000, "US", "SAS Institute", "institutional"),
        ("bil_gordon_moore_estate", "Gordon Moore Estate", 7000000000, "US", "Intel co-founder", "institutional"),
        ("bil_henry_nicholas", "Henry Nicholas III", 5000000000, "US", "Broadcom co-founder", "institutional"),
        ("bil_david_duffield", "David Duffield", 8000000000, "US", "Workday", "institutional"),
        ("bil_bob_parsons", "Bob Parsons", 3000000000, "US", "GoDaddy", "institutional"),
        ("bil_john_malone", "John Malone", 10000000000, "US", "Liberty Media", "institutional"),
        ("bil_rupert_johnson", "Rupert Johnson Jr", 5000000000, "US", "Franklin Templeton", "institutional"),
        ("bil_charles_johnson", "Charles Johnson", 8000000000, "US", "Franklin Templeton", "institutional"),
        ("bil_james_sorenson", "James Sorenson Estate", 4000000000, "US", "Medical devices", "institutional"),
        ("bil_richard_devos_estate", "Dick DeVos", 6000000000, "US", "Amway", "institutional"),
        ("bil_dan_gilbert", "Dan Gilbert", 22000000000, "US", "Quicken Loans/Rocket Mortgage", "regional"),
        ("bil_joe_mansueto", "Joe Mansueto", 5000000000, "US", "Morningstar", "institutional"),
        ("bil_andrew_beal", "Andrew Beal", 12000000000, "US", "Beal Financial Holdings", "regional"),
        ("bil_todd_christopher", "Todd Christopher", 3000000000, "US", "Investments", "institutional"),
        ("bil_shahid_khan", "Shahid Khan", 12000000000, "US", "Flex-N-Gate, Jacksonville Jaguars", "regional"),
        ("bil_marc_benioff", "Marc Benioff", 10000000000, "US", "Salesforce", "institutional"),
        ("bil_bob_iger", "Bob Iger", 1000000000, "US", "Disney", "institutional"),
        ("bil_jeffrey_sprecher", "Jeffrey Sprecher", 2000000000, "US", "Intercontinental Exchange", "institutional"),
        ("bil_kelly_loeffler", "Kelly Loeffler", 1000000000, "US", "Intercontinental Exchange", "institutional"),
        ("bil_leon_cooperman", "Leon Cooperman", 3000000000, "US", "Omega Advisors", "institutional"),
        ("bil_nelson_peltz", "Nelson Peltz", 2000000000, "US", "Trian Partners", "institutional"),
        ("bil_ron_baron", "Ron Baron", 5000000000, "US", "Baron Capital", "institutional"),
        ("bil_jeff_yass", "Jeff Yass", 28000000000, "US", "Susquehanna International", "regional"),
        ("bil_bob_kraft2", "Jonathan Kraft", 5000000000, "US", "Kraft Group", "institutional"),
        ("bil_jim_irsay", "Jim Irsay", 4000000000, "US", "Indianapolis Colts", "institutional"),
        ("bil_stephen_bisciotti", "Steve Bisciotti", 7000000000, "US", "Baltimore Ravens", "institutional"),
        ("bil_woody_johnson", "Woody Johnson", 5000000000, "US", "NY Jets, Johnson & Johnson", "institutional"),
        ("bil_arthur_blank", "Arthur Blank", 8000000000, "US", "Home Depot, Atlanta Falcons", "institutional"),
        ("bil_glen_taylor", "Glen Taylor", 3000000000, "US", "Minnesota Timberwolves", "institutional"),
        ("bil_steve_ballmer2", "Steve Ballmer", 107000000000, "US", "LA Clippers, Microsoft", "sovereign"),
        ("bil_joe_tsai", "Joe Tsai", 10000000000, "US", "Brooklyn Nets, Alibaba", "institutional"),
        ("bil_tilman_fertitta", "Tilman Fertitta", 8000000000, "US", "Landry's, Houston Rockets", "institutional"),
        ("bil_ted_lerner_estate", "Ted Lerner Estate", 6000000000, "US", "Washington Nationals", "institutional"),
        ("bil_mitchell_rales", "Mitchell Rales", 7000000000, "US", "Danaher", "institutional"),
        ("bil_steven_rales", "Steven Rales", 7000000000, "US", "Danaher", "institutional"),
        ("bil_herb_simon", "Herb Simon", 4000000000, "US", "Simon Property Group", "institutional"),
        ("bil_james_leprino", "James Leprino", 4000000000, "US", "Leprino Foods", "institutional"),
        ("bil_ira_rennert", "Ira Rennert", 5000000000, "US", "Renco Group", "institutional"),
        ("bil_isaac_perlmutter", "Isaac Perlmutter", 5000000000, "US", "Marvel, Disney", "institutional"),
        ("bil_henry_samueli", "Henry Samueli", 8000000000, "US", "Broadcom co-founder", "institutional"),
        ("bil_gabe_newell", "Gabe Newell", 10000000000, "US", "Valve/Steam", "institutional"),
        ("bil_tim_sweeney", "Tim Sweeney", 10000000000, "US", "Epic Games", "institutional"),
        ("bil_bobby_kotick", "Bobby Kotick", 1000000000, "US", "Activision Blizzard", "institutional"),
        ("bil_tobi_lutke", "Tobi Lutke", 10000000000, "Canada", "Shopify", "institutional"),
        ("bil_garrett_camp2", "Garrett Camp", 3000000000, "Canada", "Uber co-founder", "institutional"),
        ("bil_chip_wilson", "Chip Wilson", 8000000000, "Canada", "Lululemon", "institutional"),
        ("bil_peter_gilgan", "Peter Gilgan", 5000000000, "Canada", "Mattamy Homes", "institutional"),
        ("bil_clay_riddell_estate", "Sue Riddell Rose", 2000000000, "Canada", "Paramount Resources", "institutional"),
        ("bil_jimmy_pattison", "Jimmy Pattison", 12000000000, "Canada", "Jim Pattison Group", "regional"),
        ("bil_emanuele_marazzi", "Emanuele Marazzi", 2000000000, "Italy", "Ceramics", "institutional"),
        ("bil_del_vecchio_estate", "Leonardo Del Vecchio Estate", 27000000000, "Italy", "EssilorLuxottica", "regional"),
        ("bil_massimiliana_landini", "Massimiliana Landini Aleotti", 10000000000, "Italy", "Menarini Pharma", "institutional"),
        ("bil_patrizio_bertelli", "Patrizio Bertelli", 3000000000, "Italy", "Prada", "institutional"),
        ("bil_augusto_perfetti", "Augusto Perfetti", 4000000000, "Italy", "Perfetti Van Melle", "institutional"),
        ("bil_tatiana_casiraghi", "Tatiana Casiraghi", 2000000000, "Italy", "Mapei", "institutional"),
        ("bil_brunello_cucinelli", "Brunello Cucinelli", 3000000000, "Italy", "Fashion", "institutional"),
        ("bil_remo_ruffini", "Remo Ruffini", 2000000000, "Italy", "Moncler", "institutional"),
        ("bil_bernard_tapie_estate", "Bernard Tapie Estate", 1000000000, "France", "Business", "institutional"),
        ("bil_alain_merieux", "Alain Merieux", 8000000000, "France", "bioMerieux", "institutional"),
        ("bil_emmanuel_besnier", "Emmanuel Besnier", 24000000000, "France", "Lactalis", "regional"),
        ("bil_gerard_mulliez", "Gerard Mulliez", 25000000000, "France", "Auchan", "regional"),
        ("bil_pierre_castel", "Pierre Castel", 10000000000, "France", "Wine, Beer", "institutional"),
        ("bil_marc_ladreit", "Marc Ladreit de Lacharriere", 3000000000, "France", "Fimalac", "institutional"),
        ("bil_dominique_de_la_rochefoucauld", "Frederic de Mevius", 4000000000, "Belgium", "AB InBev", "institutional"),
        ("bil_alexandre_van_damme", "Alexandre Van Damme", 5000000000, "Belgium", "AB InBev", "institutional"),
        ("bil_jorge_perez", "Jorge Perez", 3000000000, "US", "Related Group", "institutional"),
        ("bil_dmitry_rybolovlev", "Dmitry Rybolovlev", 7000000000, "Russia", "Investments", "institutional"),
        ("bil_pyotr_aven", "Pyotr Aven", 4000000000, "Russia", "Alfa Group", "institutional"),
        ("bil_arkady_rotenberg", "Arkady Rotenberg", 3000000000, "Russia", "SMP Bank", "institutional"),
        ("bil_boris_rotenberg", "Boris Rotenberg", 2000000000, "Russia", "SMP Bank", "institutional"),
        ("bil_oleg_deripaska", "Oleg Deripaska", 4000000000, "Russia", "Rusal", "institutional"),
        ("bil_oleg_tinkov", "Oleg Tinkov", 3000000000, "Russia", "Tinkoff Bank", "institutional"),
        ("bil_german_khan", "German Khan", 8000000000, "Russia", "Alfa Group", "institutional"),
        ("bil_yuri_milner", "Yuri Milner", 7000000000, "Russia", "DST Global", "institutional"),
        ("bil_joseph_lau", "Joseph Lau", 14000000000, "Hong Kong", "Chinese Estates", "regional"),
        ("bil_lui_che_woo", "Lui Che Woo", 12000000000, "Hong Kong", "Galaxy Entertainment", "regional"),
        ("bil_cheng_kar_shun", "Adrian Cheng", 4000000000, "Hong Kong", "New World Development", "institutional"),
        ("bil_richard_li", "Richard Li", 5000000000, "Hong Kong", "PCCW", "institutional"),
        ("bil_ronnie_chan", "Ronnie Chan", 3000000000, "Hong Kong", "Hang Lung Properties", "institutional"),
        ("bil_lee_kun_hee_estate", "Lee Kun-hee Estate", 20000000000, "South Korea", "Samsung", "regional"),
        ("bil_chung_mong_koo", "Chung Mong-Koo", 4000000000, "South Korea", "Hyundai Motor", "institutional"),
        ("bil_shin_dong_bin", "Shin Dong-bin", 3000000000, "South Korea", "Lotte Group", "institutional"),
        ("bil_cho_jung_ho", "Cho Won-tae", 2000000000, "South Korea", "Korean Air", "institutional"),
        ("bil_koo_kwang_mo", "Koo Kwang-mo", 4000000000, "South Korea", "LG Group", "institutional"),
        ("bil_chung_yong_jin", "Chung Yong-jin", 2000000000, "South Korea", "Shinsegae", "institutional"),
        ("bil_fujio_mitarai", "Fujio Mitarai", 2000000000, "Japan", "Canon", "institutional"),
        ("bil_akio_toyoda", "Akio Toyoda", 2000000000, "Japan", "Toyota", "institutional"),
        ("bil_shigenobu_nagamori", "Shigenobu Nagamori", 6000000000, "Japan", "Nidec", "institutional"),
        ("bil_tadashi_yanai2", "Tadashi Yanai", 39000000000, "Japan", "Fast Retailing", "sovereign"),
        ("bil_taikichiro_mori_heirs", "Akira Mori", 8000000000, "Japan", "Mori Trust", "institutional"),
        ("bil_kunio_busujima", "Kunio Busujima", 5000000000, "Japan", "Sankyo", "institutional"),
        ("bil_han_hung_kuo", "Han Kuo-yu", 2000000000, "Taiwan", "Politics/Business", "institutional"),
        ("bil_tsai_hong_tu", "Tsai Hong-tu", 3000000000, "Taiwan", "Cathay Financial", "institutional"),
        ("bil_lin_baiili", "Lin Bai-li", 4000000000, "Taiwan", "Quanta Computer", "institutional"),
        ("bil_morris_chang", "Morris Chang", 3000000000, "Taiwan", "TSMC founder", "institutional"),
        ("bil_pierre_omidyar", "Pierre Omidyar", 9000000000, "US", "eBay", "institutional"),
        ("bil_meg_whitman", "Meg Whitman", 4000000000, "US", "eBay, HP", "institutional"),
        ("bil_scott_cook", "Scott Cook", 7000000000, "US", "Intuit", "institutional"),
        ("bil_david_filo", "David Filo", 5000000000, "US", "Yahoo", "institutional"),
        ("bil_min_kao", "Min Kao", 5000000000, "US", "Garmin", "institutional"),
        ("bil_hank_greenberg", "Hank Greenberg", 2000000000, "US", "AIG", "institutional"),
        ("bil_wilbur_ross", "Wilbur Ross", 1000000000, "US", "WL Ross & Co", "institutional"),
        ("bil_howard_schultz", "Howard Schultz", 4000000000, "US", "Starbucks", "institutional"),
        ("bil_charles_dolan", "Charles Dolan", 5000000000, "US", "Cablevision", "institutional"),
        ("bil_james_dolan", "James Dolan", 2000000000, "US", "MSG, NY Knicks", "institutional"),
        ("bil_sumner_redstone_estate", "Shari Redstone", 4000000000, "US", "Paramount, ViacomCBS", "institutional"),
        ("bil_john_henry", "John Henry", 5000000000, "US", "Red Sox, Liverpool FC", "institutional"),
        ("bil_tom_gores", "Tom Gores", 7000000000, "US", "Platinum Equity", "institutional"),
        ("bil_alec_gores", "Alec Gores", 4000000000, "US", "Gores Group", "institutional"),
        ("bil_noam_gottesman", "Noam Gottesman", 4000000000, "UK", "GLG Partners", "institutional"),
        ("bil_joe_lewis", "Joe Lewis", 6000000000, "UK", "Tottenham Hotspur", "institutional"),
        ("bil_peter_hargreaves", "Peter Hargreaves", 3000000000, "UK", "Hargreaves Lansdown", "institutional"),
        ("bil_michael_platt", "Michael Platt", 15000000000, "UK", "BlueCrest Capital", "regional"),
        ("bil_alan_howard", "Alan Howard", 4000000000, "UK", "Brevan Howard", "institutional"),
        ("bil_chris_hohn", "Chris Hohn", 8000000000, "UK", "TCI Fund", "institutional"),
        ("bil_joe_lewis2", "Joe Lewis", 6000000000, "UK", "Tavistock Group", "institutional"),
        ("bil_gianluigi_aponte", "Gianluigi Aponte", 30000000000, "Switzerland", "MSC", "regional"),
        ("bil_jorge_moll2", "Jorge Moll Filho", 12000000000, "Brazil", "Rede D'Or", "regional"),
        ("bil_vicky_safra", "Vicky Safra", 9000000000, "Brazil", "Safra Banking", "institutional"),
        ("bil_jose_joao_abdalla", "Jose Joao Abdalla Filho", 3000000000, "Brazil", "Caemi", "institutional"),
        ("bil_carlos_alberto_sicupira", "Carlos Alberto Sicupira", 3000000000, "Brazil", "3G Capital", "institutional"),
        ("bil_andre_esteves", "Andre Esteves", 3000000000, "Brazil", "BTG Pactual", "institutional"),
        ("bil_rubens_menin", "Rubens Menin", 4000000000, "Brazil", "MRV Engineering", "institutional"),
        ("bil_antonio_ermirio_estate", "Ermirio de Moraes Family", 7000000000, "Brazil", "Votorantim", "institutional"),
        ("bil_mohammed_al_fayed_estate", "Mohammed Al Fayed Estate", 2000000000, "Egypt", "Harrods (former)", "institutional"),
        ("bil_najib_mikati", "Najib Mikati", 3000000000, "Lebanon", "Telecom", "institutional"),
        ("bil_nicolas_puech", "Nicolas Puech", 10000000000, "France", "Hermes", "institutional"),
        ("bil_frederic_de_mevius", "Frederic de Mevius", 4000000000, "Belgium", "AB InBev", "institutional"),
        ("bil_jorge_gallardo", "Jorge Gallardo", 3000000000, "Spain", "Almirall", "institutional"),
        ("bil_juan_roig", "Juan Roig", 10000000000, "Spain", "Mercadona", "institutional"),
        ("bil_florentino_perez", "Florentino Perez", 2000000000, "Spain", "ACS, Real Madrid", "institutional"),
        ("bil_sergio_bucher", "Hortensia Herrero", 6000000000, "Spain", "Mercadona", "institutional"),
        ("bil_luis_carlos_sarmiento2", "Luis Carlos Sarmiento Angulo", 10000000000, "Colombia", "Grupo Aval", "institutional"),
        ("bil_carlos_ardila_estate", "Carlos Ardila Lulle Estate", 3000000000, "Colombia", "Postobon", "institutional"),
        ("bil_jean_salata", "Prakash Hinduja", 16000000000, "Switzerland", "Hinduja Group", "regional"),
        ("bil_apoorva_mehta", "Apoorva Mehta", 1500000000, "US", "Instacart", "institutional"),
        ("bil_fidji_simo", "Fidji Simo", 1000000000, "US", "Instacart CEO", "institutional"),
        ("bil_frank_slootman", "Frank Slootman", 2000000000, "US", "Snowflake", "institutional"),
        ("bil_jay_chaudhry", "Jay Chaudhry", 14000000000, "US", "Zscaler", "regional"),
        ("bil_nikesh_arora", "Nikesh Arora", 1000000000, "US", "Palo Alto Networks", "institutional"),
        ("bil_pony_zheng", "Zheng Yonggang", 2000000000, "China", "Shanshan Group", "institutional"),
        ("bil_liu_yiqian", "Liu Yiqian", 3000000000, "China", "Sunline Group", "institutional"),
        ("bil_xu_jiayin", "Xu Jiayin", 1000000000, "China", "Evergrande", "institutional"),
        ("bil_pan_zhengmin", "Pan Zhengmin", 3000000000, "China", "AAC Technologies", "institutional"),
        ("bil_jack_ma_joseph", "Joseph Tsai", 10000000000, "China", "Alibaba co-founder", "institutional"),
        ("bil_zhou_hongyi", "Zhou Hongyi", 3000000000, "China", "360 Security", "institutional"),
        ("bil_li_ning", "Li Ning", 4000000000, "China", "Li Ning sportswear", "institutional"),
        ("bil_ding_shizhong", "Ding Shizhong", 7000000000, "China", "ANTA Sports", "institutional"),
        ("bil_cao_dewang", "Cao Dewang", 4000000000, "China", "Fuyao Glass", "institutional"),
        ("bil_wang_wenyin", "Wang Wenyin", 8000000000, "China", "Amer International Group", "institutional"),
        ("bil_xu_shihui", "Xu Shihui", 3000000000, "China", "Nongfu Spring", "institutional"),
        ("bil_yan_hao", "Yan Hao", 3000000000, "China", "Pacific Construction", "institutional"),
        ("bil_lu_zhiqiang", "Lu Zhiqiang", 4000000000, "China", "China Oceanwide", "institutional"),
        ("bil_sun_hongbin", "Sun Hongbin", 2000000000, "China", "Sunac China", "institutional"),
        ("bil_cheng_xue", "Cheng Xue", 5000000000, "China", "Tongwei Group", "institutional"),
        ("bil_lu_guanqiu_estate", "Lu Weiding", 4000000000, "China", "Wanxiang Group", "institutional"),
        ("bil_liu_hanyuan", "Liu Hanyuan", 6000000000, "China", "Tongwei", "institutional"),
        ("bil_mukesh_jhunjhunwala_estate", "Rekha Jhunjhunwala", 5000000000, "India", "Investments", "institutional"),
        ("bil_bajaj_family", "Rajiv Bajaj", 15000000000, "India", "Bajaj Auto", "regional"),
        ("bil_gautam_singhania", "Gautam Singhania", 2000000000, "India", "Raymond Group", "institutional"),
        ("bil_ravi_jaipuria", "Ravi Jaipuria", 5000000000, "India", "RJ Corp/Pepsi bottler", "institutional"),
        ("bil_yusuffali_ma", "Yusuffali MA", 8000000000, "India", "Lulu Group", "institutional"),
        ("bil_kiran_mazumdar_shaw", "Kiran Mazumdar-Shaw", 4000000000, "India", "Biocon", "institutional"),
        ("bil_pankaj_patel", "Pankaj Patel", 5000000000, "India", "Cadila Healthcare", "institutional"),
        ("bil_byju_raveendran", "Byju Raveendran", 1000000000, "India", "Byju's", "institutional"),
        ("bil_tony_fernandes", "Tony Fernandes", 1000000000, "Malaysia", "AirAsia", "institutional"),
        ("bil_lim_kok_thay", "Lim Kok Thay", 6000000000, "Malaysia", "Genting Group", "institutional"),
        ("bil_ananda_krishnan", "Ananda Krishnan", 7000000000, "Malaysia", "Maxis, Astro", "institutional"),
        ("bil_lucio_tan", "Lucio Tan", 3000000000, "Philippines", "Tobacco, Beer, Banking", "institutional"),
        ("bil_jaime_zobel", "Jaime Zobel de Ayala", 4000000000, "Philippines", "Ayala Corp", "institutional"),
        ("bil_ramon_ang", "Ramon Ang", 4000000000, "Philippines", "San Miguel Corp", "institutional"),
        ("bil_john_gokongwei_jr_estate", "Lance Gokongwei", 3000000000, "Philippines", "JG Summit", "institutional"),
        ("bil_putera_sampoerna", "Putera Sampoerna", 4000000000, "Indonesia", "Sampoerna", "institutional"),
        ("bil_eka_tjipta_widjaja_estate", "Franky Widjaja", 6000000000, "Indonesia", "Sinar Mas Group", "institutional"),
        ("bil_sukanto_tanoto", "Sukanto Tanoto", 5000000000, "Indonesia", "Royal Golden Eagle", "institutional"),
        ("bil_tahir", "Tahir", 4000000000, "Indonesia", "Mayapada Group", "institutional"),
        ("bil_abdulla_al_ghurair", "Abdulla Al Ghurair", 4000000000, "UAE", "Mashreq Bank", "institutional"),
        ("bil_majid_al_futtaim_estate", "Majid Al Futtaim Estate", 7000000000, "UAE", "Retail", "institutional"),
        ("bil_hussain_sajwani", "Hussain Sajwani", 4000000000, "UAE", "DAMAC Properties", "institutional"),
        ("bil_abdulaziz_al_ghurair", "Abdulaziz Al Ghurair", 3000000000, "UAE", "Mashreq Bank", "institutional"),
        ("bil_mohammed_al_amoudi2", "Mohammed Al Amoudi", 7000000000, "Saudi Arabia", "MIDROC", "institutional"),
        ("bil_saleh_kamel_estate", "Saleh Kamel Estate", 2000000000, "Saudi Arabia", "Dallah Albaraka", "institutional"),
        ("bil_al_walid_al_hariri", "Saad Hariri", 2000000000, "Lebanon", "Saudi Oger", "institutional"),
        ("bil_issad_rebrab", "Issad Rebrab", 4000000000, "Algeria", "Cevital", "institutional"),
        ("bil_othman_benjelloun", "Othman Benjelloun", 3000000000, "Morocco", "BMCE Bank", "institutional"),
        ("bil_aziz_akhannouch2", "Aziz Akhannouch", 2000000000, "Morocco", "Akwa Group", "institutional"),
        ("bil_onsi_sawiris_estate", "Onsi Sawiris Estate", 2000000000, "Egypt", "Orascom", "institutional"),
        ("bil_abdulsamad_rabiu2", "Abdulsamad Rabiu", 8000000000, "Nigeria", "BUA Group", "institutional"),
        ("bil_arthur_eze", "Arthur Eze", 2000000000, "Nigeria", "Atlas Oranto Petroleum", "institutional"),
        ("bil_femi_otedola", "Femi Otedola", 2000000000, "Nigeria", "Forte Oil", "institutional"),
        ("bil_christoffel_wiese", "Christo Wiese", 2000000000, "South Africa", "Shoprite", "institutional"),
        ("bil_koos_bekker", "Koos Bekker", 3000000000, "South Africa", "Naspers", "institutional"),
        ("bil_desmond_sacco", "Desmond Sacco", 1000000000, "South Africa", "Assore", "institutional"),
        ("bil_ivan_glasenberg", "Ivan Glasenberg", 10000000000, "Switzerland", "Glencore", "institutional"),
        ("bil_thomas_schmidheiny", "Thomas Schmidheiny", 4000000000, "Switzerland", "Holcim", "institutional"),
        ("bil_hansjorg_wyss", "Hansjorg Wyss", 7000000000, "Switzerland", "Synthes", "institutional"),
        ("bil_martin_haefner", "Martin Haefner", 4000000000, "Switzerland", "Amag", "institutional"),
        ("bil_wichai_thongtang", "Wichai Thongtang", 3000000000, "Thailand", "Law, Business", "institutional"),
        ("bil_chalerm_yoovidhya", "Chalerm Yoovidhya", 24000000000, "Thailand", "Red Bull", "regional"),
        ("bil_sarath_ratanavadi", "Sarath Ratanavadi", 11000000000, "Thailand", "Gulf Energy", "institutional"),
    ]


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2: WORLD LEADERS (Heads of State + Heads of Government)
# ═══════════════════════════════════════════════════════════════════════════════

def get_world_leaders():
    """Current world leaders as of early 2026. Returns (id, name, title, country, party, tier)."""
    return [
        # ── G7 ──
        ("gov_us_trump", "Donald Trump", "President", "United States", "Republican", "sovereign"),
        ("gov_uk_starmer", "Keir Starmer", "Prime Minister", "United Kingdom", "Labour", "sovereign"),
        ("gov_uk_charles", "King Charles III", "Head of State", "United Kingdom", "Monarchy", "sovereign"),
        ("gov_france_macron", "Emmanuel Macron", "President", "France", "Renaissance", "sovereign"),
        ("gov_france_bayrou", "Francois Bayrou", "Prime Minister", "France", "MoDem", "sovereign"),
        ("gov_germany_scholz", "Olaf Scholz", "Chancellor", "Germany", "SPD", "sovereign"),
        ("gov_germany_steinmeier", "Frank-Walter Steinmeier", "President", "Germany", "SPD", "sovereign"),
        ("gov_italy_meloni", "Giorgia Meloni", "Prime Minister", "Italy", "Fratelli d'Italia", "sovereign"),
        ("gov_italy_mattarella", "Sergio Mattarella", "President", "Italy", "Independent", "sovereign"),
        ("gov_japan_ishiba", "Shigeru Ishiba", "Prime Minister", "Japan", "LDP", "sovereign"),
        ("gov_japan_emperor", "Emperor Naruhito", "Emperor", "Japan", "Imperial", "sovereign"),
        ("gov_canada_carney", "Mark Carney", "Prime Minister", "Canada", "Liberal", "sovereign"),
        # ── G20 (non-G7) ──
        ("gov_china_xi", "Xi Jinping", "President", "China", "CPC", "sovereign"),
        ("gov_china_li", "Li Qiang", "Premier", "China", "CPC", "sovereign"),
        ("gov_india_modi", "Narendra Modi", "Prime Minister", "India", "BJP", "sovereign"),
        ("gov_india_murmu", "Droupadi Murmu", "President", "India", "BJP", "sovereign"),
        ("gov_brazil_lula", "Luiz Inacio Lula da Silva", "President", "Brazil", "PT", "sovereign"),
        ("gov_russia_putin", "Vladimir Putin", "President", "Russia", "United Russia", "sovereign"),
        ("gov_russia_mishustin", "Mikhail Mishustin", "Prime Minister", "Russia", "United Russia", "sovereign"),
        ("gov_australia_albanese", "Anthony Albanese", "Prime Minister", "Australia", "Labor", "sovereign"),
        ("gov_south_korea_yoon", "Yoon Suk-yeol", "President", "South Korea", "PPP", "sovereign"),
        ("gov_mexico_sheinbaum", "Claudia Sheinbaum", "President", "Mexico", "MORENA", "sovereign"),
        ("gov_indonesia_prabowo", "Prabowo Subianto", "President", "Indonesia", "Gerindra", "sovereign"),
        ("gov_turkey_erdogan", "Recep Tayyip Erdogan", "President", "Turkey", "AKP", "sovereign"),
        ("gov_saudi_salman", "King Salman", "King", "Saudi Arabia", "Monarchy", "sovereign"),
        ("gov_saudi_mbs", "Mohammed bin Salman", "Crown Prince & PM", "Saudi Arabia", "Monarchy", "sovereign"),
        ("gov_argentina_milei", "Javier Milei", "President", "Argentina", "La Libertad Avanza", "sovereign"),
        ("gov_south_africa_ramaphosa", "Cyril Ramaphosa", "President", "South Africa", "ANC", "sovereign"),
        # ── Europe (non-G7) ──
        ("gov_spain_sanchez", "Pedro Sanchez", "Prime Minister", "Spain", "PSOE", "regional"),
        ("gov_spain_felipe", "King Felipe VI", "King", "Spain", "Monarchy", "regional"),
        ("gov_netherlands_schoof", "Dick Schoof", "Prime Minister", "Netherlands", "Independent", "regional"),
        ("gov_netherlands_willem", "King Willem-Alexander", "King", "Netherlands", "Monarchy", "regional"),
        ("gov_belgium_de_croo", "Alexander De Croo", "Prime Minister", "Belgium", "Open VLD", "regional"),
        ("gov_belgium_philippe", "King Philippe", "King", "Belgium", "Monarchy", "regional"),
        ("gov_sweden_kristersson", "Ulf Kristersson", "Prime Minister", "Sweden", "Moderates", "regional"),
        ("gov_sweden_carl_gustaf", "King Carl XVI Gustaf", "King", "Sweden", "Monarchy", "regional"),
        ("gov_norway_store", "Jonas Gahr Store", "Prime Minister", "Norway", "Labour", "regional"),
        ("gov_norway_harald", "King Harald V", "King", "Norway", "Monarchy", "regional"),
        ("gov_denmark_frederiksen", "Mette Frederiksen", "Prime Minister", "Denmark", "Social Democrats", "regional"),
        ("gov_denmark_frederik", "King Frederik X", "King", "Denmark", "Monarchy", "regional"),
        ("gov_finland_orpo", "Petteri Orpo", "Prime Minister", "Finland", "NCP", "regional"),
        ("gov_finland_stubb", "Alexander Stubb", "President", "Finland", "NCP", "regional"),
        ("gov_poland_tusk", "Donald Tusk", "Prime Minister", "Poland", "Civic Platform", "regional"),
        ("gov_poland_duda", "Andrzej Duda", "President", "Poland", "PiS", "regional"),
        ("gov_austria_nehammer", "Karl Nehammer", "Chancellor", "Austria", "OVP", "regional"),
        ("gov_austria_vanderbellen", "Alexander Van der Bellen", "President", "Austria", "Greens", "regional"),
        ("gov_switzerland_keller", "Karin Keller-Sutter", "President", "Switzerland", "FDP", "regional"),
        ("gov_portugal_montenegro", "Luis Montenegro", "Prime Minister", "Portugal", "PSD", "regional"),
        ("gov_portugal_rebelo", "Marcelo Rebelo de Sousa", "President", "Portugal", "PSD", "regional"),
        ("gov_ireland_harris", "Simon Harris", "Taoiseach", "Ireland", "Fine Gael", "regional"),
        ("gov_ireland_higgins", "Michael D. Higgins", "President", "Ireland", "Labour", "regional"),
        ("gov_greece_mitsotakis", "Kyriakos Mitsotakis", "Prime Minister", "Greece", "New Democracy", "regional"),
        ("gov_czech_fiala", "Petr Fiala", "Prime Minister", "Czech Republic", "ODS", "regional"),
        ("gov_czech_pavel", "Petr Pavel", "President", "Czech Republic", "Independent", "regional"),
        ("gov_romania_ciolacu", "Marcel Ciolacu", "Prime Minister", "Romania", "PSD", "regional"),
        ("gov_hungary_orban", "Viktor Orban", "Prime Minister", "Hungary", "Fidesz", "regional"),
        ("gov_hungary_sulyok", "Tamas Sulyok", "President", "Hungary", "Independent", "regional"),
        ("gov_croatia_plenkovic", "Andrej Plenkovic", "Prime Minister", "Croatia", "HDZ", "regional"),
        ("gov_croatia_milanovic", "Zoran Milanovic", "President", "Croatia", "SDP", "regional"),
        ("gov_serbia_vucic", "Aleksandar Vucic", "President", "Serbia", "SNS", "regional"),
        ("gov_ukraine_zelensky", "Volodymyr Zelensky", "President", "Ukraine", "Servant of the People", "regional"),
        ("gov_ukraine_shmyhal", "Denys Shmyhal", "Prime Minister", "Ukraine", "Servant of the People", "regional"),
        ("gov_bulgaria_glavchev", "Dimitar Glavchev", "Prime Minister", "Bulgaria", "GERB", "regional"),
        ("gov_slovakia_fico", "Robert Fico", "Prime Minister", "Slovakia", "SMER-SD", "regional"),
        ("gov_slovenia_golob", "Robert Golob", "Prime Minister", "Slovenia", "Freedom Movement", "regional"),
        ("gov_lithuania_simonyte", "Ingrida Simonyte", "Prime Minister", "Lithuania", "Homeland Union", "regional"),
        ("gov_latvia_silina", "Evika Silina", "Prime Minister", "Latvia", "New Unity", "regional"),
        ("gov_estonia_michal", "Kristen Michal", "Prime Minister", "Estonia", "Reform Party", "regional"),
        ("gov_iceland_jakobsdottir", "Katrin Jakobsdottir", "Prime Minister", "Iceland", "Left-Green", "regional"),
        ("gov_malta_abela", "Robert Abela", "Prime Minister", "Malta", "Labour", "regional"),
        ("gov_cyprus_christodoulides", "Nikos Christodoulides", "President", "Cyprus", "Independent", "regional"),
        ("gov_luxembourg_frieden", "Luc Frieden", "Prime Minister", "Luxembourg", "CSV", "regional"),
        ("gov_montenegro_spajic", "Milojko Spajic", "Prime Minister", "Montenegro", "PES", "regional"),
        ("gov_north_macedonia_mickoski", "Hristijan Mickoski", "Prime Minister", "North Macedonia", "VMRO-DPMNE", "regional"),
        ("gov_albania_rama", "Edi Rama", "Prime Minister", "Albania", "PS", "regional"),
        ("gov_kosovo_kurti", "Albin Kurti", "Prime Minister", "Kosovo", "Vetevendosje", "regional"),
        ("gov_bosnia_becirovic", "Denis Becirovic", "Presidency Chair", "Bosnia and Herzegovina", "SDP", "regional"),
        ("gov_moldova_sandu", "Maia Sandu", "President", "Moldova", "PAS", "regional"),
        ("gov_georgia_kobakhidze", "Irakli Kobakhidze", "Prime Minister", "Georgia", "Georgian Dream", "regional"),
        ("gov_armenia_pashinyan", "Nikol Pashinyan", "Prime Minister", "Armenia", "Civil Contract", "regional"),
        ("gov_azerbaijan_aliyev", "Ilham Aliyev", "President", "Azerbaijan", "YAP", "regional"),
        ("gov_belarus_lukashenko", "Alexander Lukashenko", "President", "Belarus", "Independent", "regional"),
        # ── Middle East ──
        ("gov_uae_mbz", "Sheikh Mohamed bin Zayed", "President", "UAE", "Monarchy", "sovereign"),
        ("gov_uae_maktoum", "Sheikh Mohammed bin Rashid Al Maktoum", "PM & VP", "UAE", "Monarchy", "sovereign"),
        ("gov_qatar_tamim", "Sheikh Tamim bin Hamad Al Thani", "Emir", "Qatar", "Monarchy", "sovereign"),
        ("gov_kuwait_mishal", "Sheikh Mishal Al-Ahmad Al-Jaber Al-Sabah", "Emir", "Kuwait", "Monarchy", "regional"),
        ("gov_bahrain_hamad", "King Hamad bin Isa Al Khalifa", "King", "Bahrain", "Monarchy", "regional"),
        ("gov_oman_haitham", "Sultan Haitham bin Tariq", "Sultan", "Oman", "Monarchy", "regional"),
        ("gov_jordan_abdullah", "King Abdullah II", "King", "Jordan", "Monarchy", "regional"),
        ("gov_iran_khamenei", "Ali Khamenei", "Supreme Leader", "Iran", "Principlist", "sovereign"),
        ("gov_iran_pezeshkian", "Masoud Pezeshkian", "President", "Iran", "Reformist", "regional"),
        ("gov_iraq_sudani", "Mohammed Shia al-Sudani", "Prime Minister", "Iraq", "Coordination Framework", "regional"),
        ("gov_iraq_rashid", "Abdul Latif Rashid", "President", "Iraq", "PUK", "regional"),
        ("gov_israel_netanyahu", "Benjamin Netanyahu", "Prime Minister", "Israel", "Likud", "sovereign"),
        ("gov_israel_herzog", "Isaac Herzog", "President", "Israel", "Labour", "regional"),
        ("gov_lebanon_aoun", "Joseph Aoun", "President", "Lebanon", "Independent", "regional"),
        ("gov_syria_sharaa", "Ahmad al-Sharaa", "De facto leader", "Syria", "HTS", "regional"),
        ("gov_yemen_rashad", "Rashad al-Alimi", "Presidential Council Chair", "Yemen", "GPC", "regional"),
        ("gov_palestine_abbas", "Mahmoud Abbas", "President", "Palestine", "Fatah", "regional"),
        # ── Africa ──
        ("gov_egypt_sisi", "Abdel Fattah el-Sisi", "President", "Egypt", "Military", "regional"),
        ("gov_nigeria_tinubu", "Bola Tinubu", "President", "Nigeria", "APC", "regional"),
        ("gov_ethiopia_ahmed", "Abiy Ahmed", "Prime Minister", "Ethiopia", "PP", "regional"),
        ("gov_kenya_ruto", "William Ruto", "President", "Kenya", "Kenya Kwanza", "regional"),
        ("gov_tanzania_hassan", "Samia Suluhu Hassan", "President", "Tanzania", "CCM", "regional"),
        ("gov_ghana_mahama", "John Mahama", "President", "Ghana", "NDC", "regional"),
        ("gov_senegal_faye", "Bassirou Diomaye Faye", "President", "Senegal", "PASTEF", "regional"),
        ("gov_drc_tshisekedi", "Felix Tshisekedi", "President", "DR Congo", "UDPS", "regional"),
        ("gov_morocco_akhannouch", "Aziz Akhannouch", "Prime Minister", "Morocco", "RNI", "regional"),
        ("gov_morocco_mohammed", "King Mohammed VI", "King", "Morocco", "Monarchy", "regional"),
        ("gov_algeria_tebboune", "Abdelmadjid Tebboune", "President", "Algeria", "Independent", "regional"),
        ("gov_tunisia_saied", "Kais Saied", "President", "Tunisia", "Independent", "regional"),
        ("gov_libya_dbeibeh", "Abdul Hamid Dbeibeh", "PM (GNU)", "Libya", "GNU", "regional"),
        ("gov_sudan_burhan", "Abdel Fattah al-Burhan", "De facto leader", "Sudan", "Military", "regional"),
        ("gov_angola_lourenco", "Joao Lourenco", "President", "Angola", "MPLA", "regional"),
        ("gov_mozambique_nyusi", "Daniel Chapo", "President", "Mozambique", "FRELIMO", "regional"),
        ("gov_uganda_museveni", "Yoweri Museveni", "President", "Uganda", "NRM", "regional"),
        ("gov_rwanda_kagame", "Paul Kagame", "President", "Rwanda", "RPF", "regional"),
        ("gov_cameroon_biya", "Paul Biya", "President", "Cameroon", "CPDM", "regional"),
        ("gov_ivory_coast_ouattara", "Alassane Ouattara", "President", "Ivory Coast", "RDR", "regional"),
        ("gov_madagascar_rajoelina", "Andry Rajoelina", "President", "Madagascar", "TGV", "regional"),
        ("gov_zimbabwe_mnangagwa", "Emmerson Mnangagwa", "President", "Zimbabwe", "ZANU-PF", "regional"),
        ("gov_zambia_hichilema", "Hakainde Hichilema", "President", "Zambia", "UPND", "regional"),
        ("gov_namibia_nandi_ndaitwah", "Netumbo Nandi-Ndaitwah", "President", "Namibia", "SWAPO", "regional"),
        ("gov_botswana_boko", "Duma Boko", "President", "Botswana", "UDC", "regional"),
        ("gov_somalia_hassan", "Hassan Sheikh Mohamud", "President", "Somalia", "UPD", "regional"),
        ("gov_mali_goita", "Assimi Goita", "Interim President", "Mali", "Military", "regional"),
        ("gov_burkina_traore", "Ibrahim Traore", "Interim President", "Burkina Faso", "Military", "regional"),
        ("gov_niger_tiani", "Abdourahamane Tiani", "De facto leader", "Niger", "Military", "regional"),
        # ── Asia-Pacific ──
        ("gov_pakistan_sharif", "Shehbaz Sharif", "Prime Minister", "Pakistan", "PML-N", "regional"),
        ("gov_pakistan_zardari", "Asif Ali Zardari", "President", "Pakistan", "PPP", "regional"),
        ("gov_bangladesh_yunus", "Muhammad Yunus", "Chief Adviser", "Bangladesh", "Independent", "regional"),
        ("gov_srilanka_dissanayake", "Anura Kumara Dissanayake", "President", "Sri Lanka", "JVP/NPP", "regional"),
        ("gov_nepal_oli", "KP Sharma Oli", "Prime Minister", "Nepal", "CPN-UML", "regional"),
        ("gov_myanmar_min_aung_hlaing", "Min Aung Hlaing", "Military leader", "Myanmar", "Military", "regional"),
        ("gov_thailand_paetongtarn", "Paetongtarn Shinawatra", "Prime Minister", "Thailand", "Pheu Thai", "regional"),
        ("gov_thailand_vajiralongkorn", "King Vajiralongkorn", "King", "Thailand", "Monarchy", "regional"),
        ("gov_vietnam_to_lam", "To Lam", "General Secretary", "Vietnam", "CPV", "regional"),
        ("gov_vietnam_luong_cuong", "Luong Cuong", "President", "Vietnam", "CPV", "regional"),
        ("gov_philippines_marcos", "Ferdinand Marcos Jr", "President", "Philippines", "PFP", "regional"),
        ("gov_malaysia_anwar", "Anwar Ibrahim", "Prime Minister", "Malaysia", "PKR", "regional"),
        ("gov_singapore_wong", "Lawrence Wong", "Prime Minister", "Singapore", "PAP", "regional"),
        ("gov_cambodia_hun_manet", "Hun Manet", "Prime Minister", "Cambodia", "CPP", "regional"),
        ("gov_laos_sonexay", "Sonexay Siphandone", "Prime Minister", "Laos", "LPRP", "regional"),
        ("gov_mongolia_oyun_erdene", "Luvsannamsrain Oyun-Erdene", "Prime Minister", "Mongolia", "MPP", "regional"),
        ("gov_north_korea_kim", "Kim Jong Un", "Supreme Leader", "North Korea", "WPK", "sovereign"),
        ("gov_taiwan_lai", "Lai Ching-te", "President", "Taiwan", "DPP", "regional"),
        ("gov_newzealand_luxon", "Christopher Luxon", "Prime Minister", "New Zealand", "National", "regional"),
        ("gov_fiji_rabuka", "Sitiveni Rabuka", "Prime Minister", "Fiji", "People's Alliance", "regional"),
        ("gov_papua_marape", "James Marape", "Prime Minister", "Papua New Guinea", "Pangu Pati", "regional"),
        # ── Central Asia ──
        ("gov_kazakhstan_tokayev", "Kassym-Jomart Tokayev", "President", "Kazakhstan", "Amanat", "regional"),
        ("gov_uzbekistan_mirziyoyev", "Shavkat Mirziyoyev", "President", "Uzbekistan", "UzLiDeP", "regional"),
        ("gov_turkmenistan_berdimuhamedow", "Serdar Berdimuhamedow", "President", "Turkmenistan", "DPT", "regional"),
        ("gov_tajikistan_rahmon", "Emomali Rahmon", "President", "Tajikistan", "PDPT", "regional"),
        ("gov_kyrgyzstan_japarov", "Sadyr Japarov", "President", "Kyrgyzstan", "Independent", "regional"),
        ("gov_afghanistan_akhundzada", "Hibatullah Akhundzada", "Supreme Leader", "Afghanistan", "Taliban", "regional"),
        # ── Americas (non-G7, non-G20) ──
        ("gov_colombia_petro", "Gustavo Petro", "President", "Colombia", "Pacto Historico", "regional"),
        ("gov_chile_boric", "Gabriel Boric", "President", "Chile", "Convergencia Social", "regional"),
        ("gov_peru_boluarte", "Dina Boluarte", "President", "Peru", "Independent", "regional"),
        ("gov_venezuela_maduro", "Nicolas Maduro", "President", "Venezuela", "PSUV", "regional"),
        ("gov_ecuador_noboa", "Daniel Noboa", "President", "Ecuador", "ADN", "regional"),
        ("gov_uruguay_orsi", "Yamandu Orsi", "President", "Uruguay", "Frente Amplio", "regional"),
        ("gov_paraguay_pena", "Santiago Pena", "President", "Paraguay", "Colorado Party", "regional"),
        ("gov_bolivia_arce", "Luis Arce", "President", "Bolivia", "MAS", "regional"),
        ("gov_panama_mulino", "Jose Raul Mulino", "President", "Panama", "Realizando Metas", "regional"),
        ("gov_costa_rica_chaves", "Rodrigo Chaves", "President", "Costa Rica", "PPSD", "regional"),
        ("gov_guatemala_arevalo", "Bernardo Arevalo", "President", "Guatemala", "Semilla", "regional"),
        ("gov_honduras_castro", "Xiomara Castro", "President", "Honduras", "LIBRE", "regional"),
        ("gov_el_salvador_bukele", "Nayib Bukele", "President", "El Salvador", "Nuevas Ideas", "regional"),
        ("gov_nicaragua_ortega", "Daniel Ortega", "President", "Nicaragua", "FSLN", "regional"),
        ("gov_cuba_diaz_canel", "Miguel Diaz-Canel", "President", "Cuba", "PCC", "regional"),
        ("gov_dominican_abinader", "Luis Abinader", "President", "Dominican Republic", "PRM", "regional"),
        ("gov_haiti_conille", "Garry Conille", "PM", "Haiti", "Independent", "regional"),
        ("gov_jamaica_holness", "Andrew Holness", "Prime Minister", "Jamaica", "JLP", "regional"),
        ("gov_trinidad_rowley", "Keith Rowley", "Prime Minister", "Trinidad and Tobago", "PNM", "regional"),
        ("gov_guyana_ali", "Irfaan Ali", "President", "Guyana", "PPP/C", "regional"),
        ("gov_suriname_santokhi", "Chan Santokhi", "President", "Suriname", "VHP", "regional"),
        ("gov_belize_briceno", "John Briceno", "Prime Minister", "Belize", "PUP", "regional"),
        ("gov_barbados_mottley", "Mia Mottley", "Prime Minister", "Barbados", "BLP", "regional"),
        ("gov_bahamas_davis", "Philip Davis", "Prime Minister", "Bahamas", "PLP", "regional"),
        # ── Pacific Islands ──
        ("gov_samoa_fiame", "Fiame Naomi Mata'afa", "Prime Minister", "Samoa", "FAST", "regional"),
        ("gov_tonga_hu_akau", "Hu'akavameiliku Siaosi Sovaleni", "Prime Minister", "Tonga", "Independent", "regional"),
        ("gov_vanuatu_kalsakau", "Charlot Salwai", "Prime Minister", "Vanuatu", "RMC", "regional"),
        ("gov_solomon_sogavare", "Jeremiah Manele", "Prime Minister", "Solomon Islands", "OUR Party", "regional"),
        ("gov_tuvalu_natano", "Feleti Teo", "Prime Minister", "Tuvalu", "Independent", "regional"),
        ("gov_kiribati_maamau", "Taneti Maamau", "President", "Kiribati", "TKB", "regional"),
        ("gov_micronesia_simina", "Wesley Simina", "President", "Micronesia", "Independent", "regional"),
        ("gov_palau_whipps", "Surangel Whipps Jr", "President", "Palau", "Independent", "regional"),
        ("gov_marshall_kabua", "Hilda Heine", "President", "Marshall Islands", "Independent", "regional"),
        # ── Additional African leaders ──
        ("gov_chad_deby", "Mahamat Idriss Deby", "President", "Chad", "MPS", "regional"),
        ("gov_central_african_touadera", "Faustin-Archange Touadera", "President", "Central African Republic", "MCU", "regional"),
        ("gov_congo_sassou", "Denis Sassou Nguesso", "President", "Republic of Congo", "PCT", "regional"),
        ("gov_gabon_oligui", "Brice Oligui Nguema", "Interim President", "Gabon", "Military", "regional"),
        ("gov_equatorial_guinea_obiang", "Teodoro Obiang", "President", "Equatorial Guinea", "PDGE", "regional"),
        ("gov_togo_gnassingbe", "Faure Gnassingbe", "President", "Togo", "UNIR", "regional"),
        ("gov_benin_talon", "Patrice Talon", "President", "Benin", "Independent", "regional"),
        ("gov_guinea_doumbouya", "Mamady Doumbouya", "Interim President", "Guinea", "Military", "regional"),
        ("gov_sierra_leone_bio", "Julius Maada Bio", "President", "Sierra Leone", "SLPP", "regional"),
        ("gov_liberia_boakai", "Joseph Boakai", "President", "Liberia", "UP", "regional"),
        ("gov_gambia_barrow", "Adama Barrow", "President", "Gambia", "NPP", "regional"),
        ("gov_guinea_bissau_embalo", "Umaro Sissoco Embalo", "President", "Guinea-Bissau", "Madem G15", "regional"),
        ("gov_mauritania_ghazouani", "Mohamed Ould Ghazouani", "President", "Mauritania", "El Insaf", "regional"),
        ("gov_mauritius_jugnauth", "Pravind Jugnauth", "Prime Minister", "Mauritius", "MSM", "regional"),
        ("gov_seychelles_ramkalawan", "Wavel Ramkalawan", "President", "Seychelles", "LDS", "regional"),
        ("gov_comoros_azali", "Azali Assoumani", "President", "Comoros", "CRC", "regional"),
        ("gov_djibouti_guelleh", "Ismail Omar Guelleh", "President", "Djibouti", "RPP", "regional"),
        ("gov_eritrea_afwerki", "Isaias Afwerki", "President", "Eritrea", "PFDJ", "regional"),
        ("gov_south_sudan_kiir", "Salva Kiir", "President", "South Sudan", "SPLM", "regional"),
        ("gov_malawi_chakwera", "Lazarus Chakwera", "President", "Malawi", "MCP", "regional"),
        ("gov_lesotho_majoro", "Sam Matekane", "Prime Minister", "Lesotho", "RFP", "regional"),
        ("gov_eswatini_dlamini", "Russell Dlamini", "Prime Minister", "Eswatini", "Monarchy", "regional"),
        # ── International Organization heads ──
        ("gov_un_guterres", "Antonio Guterres", "Secretary-General", "United Nations", "Independent", "sovereign"),
        ("gov_eu_vonderleyen", "Ursula von der Leyen", "Commission President", "European Union", "EPP", "sovereign"),
        ("gov_eu_costa", "Antonio Costa", "European Council President", "European Union", "PES", "sovereign"),
        ("gov_nato_rutte", "Mark Rutte", "Secretary General", "NATO", "VVD", "sovereign"),
        ("gov_who_tedros", "Tedros Adhanom Ghebreyesus", "Director-General", "WHO", "Independent", "regional"),
        ("gov_imf_georgieva", "Kristalina Georgieva", "Managing Director", "IMF", "Independent", "sovereign"),
        ("gov_wb_banga", "Ajay Banga", "President", "World Bank", "Independent", "sovereign"),
        ("gov_wto_okonjo_iweala", "Ngozi Okonjo-Iweala", "Director-General", "WTO", "Independent", "regional"),
    ]


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3: TOP COMPANIES (by market cap, 1000+)
# ═══════════════════════════════════════════════════════════════════════════════

def get_companies():
    """Top global companies. Returns (id, name, ticker, sector, market_cap_estimate, country, ceo)."""
    return [
        # ── Mega Cap ($500B+) ──
        ("corp_AAPL", "Apple Inc", "AAPL", "Technology", 3400000000000, "US", "Tim Cook"),
        ("corp_MSFT", "Microsoft Corp", "MSFT", "Technology", 3200000000000, "US", "Satya Nadella"),
        ("corp_NVDA", "NVIDIA Corp", "NVDA", "Technology", 3000000000000, "US", "Jensen Huang"),
        ("corp_GOOG", "Alphabet Inc", "GOOG", "Technology", 2200000000000, "US", "Sundar Pichai"),
        ("corp_AMZN", "Amazon.com Inc", "AMZN", "Technology", 2100000000000, "US", "Andy Jassy"),
        ("corp_META", "Meta Platforms Inc", "META", "Technology", 1700000000000, "US", "Mark Zuckerberg"),
        ("corp_BRK", "Berkshire Hathaway", "BRK.B", "Financial", 1100000000000, "US", "Warren Buffett"),
        ("corp_TSM", "Taiwan Semiconductor", "TSM", "Semiconductors", 900000000000, "Taiwan", "C.C. Wei"),
        ("corp_LLY", "Eli Lilly", "LLY", "Healthcare", 850000000000, "US", "David Ricks"),
        ("corp_AVGO", "Broadcom Inc", "AVGO", "Semiconductors", 800000000000, "US", "Hock Tan"),
        ("corp_JPM", "JPMorgan Chase", "JPM", "Financial", 750000000000, "US", "Jamie Dimon"),
        ("corp_TSLA", "Tesla Inc", "TSLA", "Automotive", 700000000000, "US", "Elon Musk"),
        ("corp_WMT", "Walmart Inc", "WMT", "Retail", 680000000000, "US", "Doug McMillon"),
        ("corp_V", "Visa Inc", "V", "Financial", 620000000000, "US", "Ryan McInerney"),
        ("corp_XOM", "Exxon Mobil", "XOM", "Energy", 540000000000, "US", "Darren Woods"),
        ("corp_UNH", "UnitedHealth Group", "UNH", "Healthcare", 530000000000, "US", "Andrew Witty"),
        ("corp_MA", "Mastercard", "MA", "Financial", 510000000000, "US", "Michael Miebach"),
        ("corp_ORCL", "Oracle Corp", "ORCL", "Technology", 500000000000, "US", "Safra Catz"),
        # ── Large Cap ($200B–$500B) ──
        ("corp_COST", "Costco Wholesale", "COST", "Retail", 420000000000, "US", "Ron Vachris"),
        ("corp_HD", "Home Depot", "HD", "Retail", 400000000000, "US", "Ted Decker"),
        ("corp_PG", "Procter & Gamble", "PG", "Consumer Staples", 390000000000, "US", "Jon Moeller"),
        ("corp_JNJ", "Johnson & Johnson", "JNJ", "Healthcare", 380000000000, "US", "Joaquin Duato"),
        ("corp_NFLX", "Netflix Inc", "NFLX", "Technology", 370000000000, "US", "Ted Sarandos"),
        ("corp_CRM", "Salesforce Inc", "CRM", "Technology", 310000000000, "US", "Marc Benioff"),
        ("corp_ABBV", "AbbVie Inc", "ABBV", "Healthcare", 300000000000, "US", "Robert Michael"),
        ("corp_BAC", "Bank of America", "BAC", "Financial", 360000000000, "US", "Brian Moynihan"),
        ("corp_KO", "Coca-Cola Co", "KO", "Consumer Staples", 300000000000, "US", "James Quincey"),
        ("corp_MRK", "Merck & Co", "MRK", "Healthcare", 280000000000, "US", "Robert Davis"),
        ("corp_CVX", "Chevron Corp", "CVX", "Energy", 280000000000, "US", "Mike Wirth"),
        ("corp_PEP", "PepsiCo Inc", "PEP", "Consumer Staples", 250000000000, "US", "Ramon Laguarta"),
        ("corp_ADBE", "Adobe Inc", "ADBE", "Technology", 260000000000, "US", "Shantanu Narayen"),
        ("corp_TMO", "Thermo Fisher", "TMO", "Healthcare", 240000000000, "US", "Marc Casper"),
        ("corp_AMD", "Advanced Micro Devices", "AMD", "Semiconductors", 240000000000, "US", "Lisa Su"),
        ("corp_WFC", "Wells Fargo", "WFC", "Financial", 250000000000, "US", "Charlie Scharf"),
        ("corp_CSCO", "Cisco Systems", "CSCO", "Technology", 250000000000, "US", "Chuck Robbins"),
        ("corp_LIN", "Linde plc", "LIN", "Materials", 230000000000, "US", "Sanjiv Lamba"),
        ("corp_ACN", "Accenture", "ACN", "Technology", 230000000000, "Ireland", "Julie Sweet"),
        ("corp_MCD", "McDonald's Corp", "MCD", "Consumer Discretionary", 220000000000, "US", "Chris Kempczinski"),
        ("corp_ABT", "Abbott Labs", "ABT", "Healthcare", 220000000000, "US", "Robert Ford"),
        ("corp_PM", "Philip Morris Intl", "PM", "Consumer Staples", 210000000000, "US", "Jacek Olczak"),
        ("corp_DHR", "Danaher Corp", "DHR", "Healthcare", 200000000000, "US", "Rainer Blair"),
        ("corp_INTC", "Intel Corp", "INTC", "Semiconductors", 100000000000, "US", "Pat Gelsinger"),
        ("corp_NOW", "ServiceNow", "NOW", "Technology", 210000000000, "US", "Bill McDermott"),
        ("corp_TXN", "Texas Instruments", "TXN", "Semiconductors", 200000000000, "US", "Haviv Ilan"),
        ("corp_IBM", "IBM", "IBM", "Technology", 210000000000, "US", "Arvind Krishna"),
        ("corp_QCOM", "Qualcomm Inc", "QCOM", "Semiconductors", 200000000000, "US", "Cristiano Amon"),
        ("corp_INTU", "Intuit Inc", "INTU", "Technology", 200000000000, "US", "Sasan Goodarzi"),
        ("corp_AMAT", "Applied Materials", "AMAT", "Semiconductors", 180000000000, "US", "Gary Dickerson"),
        ("corp_GE", "GE Aerospace", "GE", "Industrials", 220000000000, "US", "Larry Culp"),
        ("corp_AXP", "American Express", "AXP", "Financial", 210000000000, "US", "Steve Squeri"),
        ("corp_ISRG", "Intuitive Surgical", "ISRG", "Healthcare", 190000000000, "US", "Gary Guthart"),
        ("corp_CMCSA", "Comcast Corp", "CMCSA", "Communications", 170000000000, "US", "Brian Roberts"),
        ("corp_CAT", "Caterpillar Inc", "CAT", "Industrials", 190000000000, "US", "Jim Umpleby"),
        ("corp_VZ", "Verizon", "VZ", "Communications", 180000000000, "US", "Hans Vestberg"),
        ("corp_T", "AT&T Inc", "T", "Communications", 160000000000, "US", "John Stankey"),
        ("corp_SPGI", "S&P Global", "SPGI", "Financial", 160000000000, "US", "Martina Cheung"),
        ("corp_GS", "Goldman Sachs", "GS", "Financial", 180000000000, "US", "David Solomon"),
        ("corp_MS", "Morgan Stanley", "MS", "Financial", 170000000000, "US", "Ted Pick"),
        ("corp_BLK", "BlackRock", "BLK", "Financial", 155000000000, "US", "Larry Fink"),
        ("corp_UBER", "Uber Technologies", "UBER", "Technology", 175000000000, "US", "Dara Khosrowshahi"),
        ("corp_NEE", "NextEra Energy", "NEE", "Utilities", 170000000000, "US", "John Ketchum"),
        ("corp_PFE", "Pfizer Inc", "PFE", "Healthcare", 150000000000, "US", "Albert Bourla"),
        ("corp_UNP", "Union Pacific", "UNP", "Industrials", 150000000000, "US", "Jim Vena"),
        ("corp_RTX", "RTX Corp", "RTX", "Industrials", 160000000000, "US", "Chris Calio"),
        ("corp_BKNG", "Booking Holdings", "BKNG", "Consumer Discretionary", 160000000000, "US", "Glenn Fogel"),
        ("corp_HON", "Honeywell Intl", "HON", "Industrials", 150000000000, "US", "Vimal Kapur"),
        ("corp_LOW", "Lowe's Companies", "LOW", "Retail", 155000000000, "US", "Marvin Ellison"),
        ("corp_SYK", "Stryker Corp", "SYK", "Healthcare", 140000000000, "US", "Kevin Lobo"),
        ("corp_DE", "Deere & Company", "DE", "Industrials", 135000000000, "US", "John May"),
        ("corp_SCHW", "Charles Schwab", "SCHW", "Financial", 140000000000, "US", "Rick Wurster"),
        ("corp_ELV", "Elevance Health", "ELV", "Healthcare", 120000000000, "US", "Gail Boudreaux"),
        ("corp_ADP", "ADP Inc", "ADP", "Technology", 120000000000, "US", "Maria Black"),
        ("corp_VRTX", "Vertex Pharma", "VRTX", "Healthcare", 130000000000, "US", "Reshma Kewalramani"),
        ("corp_BMY", "Bristol-Myers Squibb", "BMY", "Healthcare", 110000000000, "US", "Chris Boerner"),
        ("corp_CI", "Cigna Group", "CI", "Healthcare", 100000000000, "US", "David Cordani"),
        ("corp_MDT", "Medtronic", "MDT", "Healthcare", 110000000000, "Ireland", "Geoff Martha"),
        ("corp_C", "Citigroup", "C", "Financial", 140000000000, "US", "Jane Fraser"),
        ("corp_CB", "Chubb Ltd", "CB", "Financial", 115000000000, "Switzerland", "Evan Greenberg"),
        ("corp_REGN", "Regeneron Pharma", "REGN", "Healthcare", 115000000000, "US", "Leonard Schleifer"),
        ("corp_SO", "Southern Company", "SO", "Utilities", 100000000000, "US", "Chris Womack"),
        ("corp_DUK", "Duke Energy", "DUK", "Utilities", 95000000000, "US", "Lynn Good"),
        ("corp_PLD", "Prologis", "PLD", "Real Estate", 115000000000, "US", "Hamid Moghadam"),
        ("corp_CL", "Colgate-Palmolive", "CL", "Consumer Staples", 80000000000, "US", "Noel Wallace"),
        ("corp_TJX", "TJX Companies", "TJX", "Retail", 130000000000, "US", "Ernie Herrman"),
        ("corp_ZTS", "Zoetis Inc", "ZTS", "Healthcare", 90000000000, "US", "Kristin Peck"),
        ("corp_ANET", "Arista Networks", "ANET", "Technology", 120000000000, "US", "Jayshree Ullal"),
        ("corp_MMC", "Marsh McLennan", "MMC", "Financial", 110000000000, "US", "John Doyle"),
        ("corp_FI", "Fiserv Inc", "FI", "Technology", 100000000000, "US", "Frank Bisignano"),
        ("corp_SHW", "Sherwin-Williams", "SHW", "Materials", 95000000000, "US", "Heidi Petz"),
        ("corp_ITW", "Illinois Tool Works", "ITW", "Industrials", 80000000000, "US", "Chris O'Herlihy"),
        ("corp_LRCX", "Lam Research", "LRCX", "Semiconductors", 110000000000, "US", "Tim Archer"),
        ("corp_KLAC", "KLA Corp", "KLAC", "Semiconductors", 100000000000, "US", "Rick Wallace"),
        ("corp_SNPS", "Synopsys Inc", "SNPS", "Technology", 85000000000, "US", "Sassine Ghazi"),
        ("corp_CDNS", "Cadence Design", "CDNS", "Technology", 85000000000, "US", "Anirudh Devgan"),
        ("corp_CME", "CME Group", "CME", "Financial", 85000000000, "US", "Terry Duffy"),
        ("corp_PANW", "Palo Alto Networks", "PANW", "Technology", 120000000000, "US", "Nikesh Arora"),
        ("corp_MCO", "Moody's Corp", "MCO", "Financial", 80000000000, "US", "Rob Fauber"),
        ("corp_ORLY", "O'Reilly Automotive", "ORLY", "Retail", 70000000000, "US", "Brad Beckham"),
        ("corp_USB", "US Bancorp", "USB", "Financial", 80000000000, "US", "Andy Cecere"),
        ("corp_TGT", "Target Corp", "TGT", "Retail", 70000000000, "US", "Brian Cornell"),
        ("corp_APH", "Amphenol Corp", "APH", "Technology", 90000000000, "US", "Adam Norwitt"),
        ("corp_PYPL", "PayPal Holdings", "PYPL", "Technology", 80000000000, "US", "Alex Chriss"),
        ("corp_MU", "Micron Technology", "MU", "Semiconductors", 100000000000, "US", "Sanjay Mehrotra"),
        ("corp_MSI", "Motorola Solutions", "MSI", "Technology", 75000000000, "US", "Greg Brown"),
        ("corp_ADI", "Analog Devices", "ADI", "Semiconductors", 110000000000, "US", "Vincent Roche"),
        ("corp_DIS", "Walt Disney Co", "DIS", "Communications", 210000000000, "US", "Bob Iger"),
        ("corp_NKE", "Nike Inc", "NKE", "Consumer Discretionary", 130000000000, "US", "Elliott Hill"),
        ("corp_SBUX", "Starbucks Corp", "SBUX", "Consumer Discretionary", 110000000000, "US", "Brian Niccol"),
        ("corp_BX", "Blackstone Inc", "BX", "Financial", 200000000000, "US", "Stephen Schwarzman"),
        ("corp_KKR", "KKR & Co", "KKR", "Financial", 120000000000, "US", "Scott Nuttall"),
        ("corp_APO", "Apollo Global", "APO", "Financial", 100000000000, "US", "Marc Rowan"),
        ("corp_ABNB", "Airbnb Inc", "ABNB", "Technology", 90000000000, "US", "Brian Chesky"),
        ("corp_CRWD", "CrowdStrike", "CRWD", "Technology", 80000000000, "US", "George Kurtz"),
        ("corp_SQ", "Block Inc", "SQ", "Technology", 50000000000, "US", "Jack Dorsey"),
        ("corp_SNAP", "Snap Inc", "SNAP", "Technology", 20000000000, "US", "Evan Spiegel"),
        ("corp_PLTR", "Palantir Technologies", "PLTR", "Technology", 120000000000, "US", "Alex Karp"),
        ("corp_SPOT", "Spotify Technology", "SPOT", "Technology", 90000000000, "Sweden", "Daniel Ek"),
        ("corp_SHOP", "Shopify Inc", "SHOP", "Technology", 130000000000, "Canada", "Tobi Lutke"),
        ("corp_COIN", "Coinbase Global", "COIN", "Financial", 50000000000, "US", "Brian Armstrong"),
        ("corp_MRVL", "Marvell Technology", "MRVL", "Semiconductors", 70000000000, "US", "Matt Murphy"),
        ("corp_DDOG", "Datadog Inc", "DDOG", "Technology", 50000000000, "US", "Olivier Pomel"),
        ("corp_SNOW", "Snowflake Inc", "SNOW", "Technology", 55000000000, "US", "Sridhar Ramaswamy"),
        ("corp_TEAM", "Atlassian Corp", "TEAM", "Technology", 55000000000, "Australia", "Mike Cannon-Brookes"),
        ("corp_WDAY", "Workday Inc", "WDAY", "Technology", 70000000000, "US", "Carl Eschenbach"),
        ("corp_ZS", "Zscaler Inc", "ZS", "Technology", 35000000000, "US", "Jay Chaudhry"),
        ("corp_DASH", "DoorDash Inc", "DASH", "Technology", 65000000000, "US", "Tony Xu"),
        ("corp_TTD", "The Trade Desk", "TTD", "Technology", 50000000000, "US", "Jeff Green"),
        ("corp_MELI", "MercadoLibre", "MELI", "Technology", 100000000000, "Argentina", "Marcos Galperin"),
        ("corp_ROKU", "Roku Inc", "ROKU", "Technology", 12000000000, "US", "Anthony Wood"),
        ("corp_NET", "Cloudflare Inc", "NET", "Technology", 35000000000, "US", "Matthew Prince"),
        ("corp_TWLO", "Twilio Inc", "TWLO", "Technology", 15000000000, "US", "Khozema Shipchandler"),
        ("corp_OKTA", "Okta Inc", "OKTA", "Technology", 17000000000, "US", "Todd McKinnon"),
        ("corp_PINS", "Pinterest Inc", "PINS", "Technology", 20000000000, "US", "Bill Ready"),
        ("corp_U", "Unity Software", "U", "Technology", 10000000000, "US", "Matt Bromberg"),
        ("corp_PATH", "UiPath Inc", "PATH", "Technology", 10000000000, "US", "Daniel Dines"),
        ("corp_RBLX", "Roblox Corp", "RBLX", "Technology", 35000000000, "US", "David Baszucki"),
        ("corp_RIVN", "Rivian Automotive", "RIVN", "Automotive", 15000000000, "US", "RJ Scaringe"),
        ("corp_LCID", "Lucid Group", "LCID", "Automotive", 7000000000, "US", "Peter Rawlinson"),
        # ── S&P 500 (continued) ──
        ("corp_MMM", "3M Company", "MMM", "Industrials", 70000000000, "US", "Bill Brown"),
        ("corp_GD", "General Dynamics", "GD", "Industrials", 80000000000, "US", "Phebe Novakovic"),
        ("corp_LMT", "Lockheed Martin", "LMT", "Industrials", 130000000000, "US", "Jim Taiclet"),
        ("corp_NOC", "Northrop Grumman", "NOC", "Industrials", 75000000000, "US", "Kathy Warden"),
        ("corp_BA", "Boeing Co", "BA", "Industrials", 120000000000, "US", "Kelly Ortberg"),
        ("corp_GEV", "GE Vernova", "GEV", "Industrials", 80000000000, "US", "Scott Strazik"),
        ("corp_EMR", "Emerson Electric", "EMR", "Industrials", 70000000000, "US", "Lal Karsanbhai"),
        ("corp_ETN", "Eaton Corp", "ETN", "Industrials", 130000000000, "Ireland", "Craig Arnold"),
        ("corp_PH", "Parker-Hannifin", "PH", "Industrials", 85000000000, "US", "Jenny Parmentier"),
        ("corp_ROK", "Rockwell Automation", "ROK", "Industrials", 35000000000, "US", "Blake Moret"),
        ("corp_CARR", "Carrier Global", "CARR", "Industrials", 60000000000, "US", "David Gitlin"),
        ("corp_FDX", "FedEx Corp", "FDX", "Industrials", 70000000000, "US", "Raj Subramaniam"),
        ("corp_UPS", "United Parcel Service", "UPS", "Industrials", 100000000000, "US", "Carol Tome"),
        ("corp_WM", "Waste Management", "WM", "Industrials", 90000000000, "US", "Jim Fish"),
        ("corp_RSG", "Republic Services", "RSG", "Industrials", 65000000000, "US", "Jon Vander Ark"),
        ("corp_CTAS", "Cintas Corp", "CTAS", "Industrials", 80000000000, "US", "Todd Schneider"),
        ("corp_FAST", "Fastenal Co", "FAST", "Industrials", 45000000000, "US", "Dan Florness"),
        ("corp_CPRT", "Copart Inc", "CPRT", "Industrials", 55000000000, "US", "Jeff Liaw"),
        ("corp_ODFL", "Old Dominion Freight", "ODFL", "Industrials", 40000000000, "US", "Marty Freeman"),
        ("corp_CSX", "CSX Corp", "CSX", "Industrials", 70000000000, "US", "Joe Hinrichs"),
        ("corp_NSC", "Norfolk Southern", "NSC", "Industrials", 55000000000, "US", "Mark George"),
        # ── Energy ──
        ("corp_COP", "ConocoPhillips", "COP", "Energy", 140000000000, "US", "Ryan Lance"),
        ("corp_SLB", "Schlumberger", "SLB", "Energy", 65000000000, "US", "Olivier Le Peuch"),
        ("corp_EOG", "EOG Resources", "EOG", "Energy", 75000000000, "US", "Ezra Yacob"),
        ("corp_PXD", "Pioneer Natural Res", "PXD", "Energy", 60000000000, "US", "Scott Sheffield"),
        ("corp_MPC", "Marathon Petroleum", "MPC", "Energy", 60000000000, "US", "Maryann Mannen"),
        ("corp_PSX", "Phillips 66", "PSX", "Energy", 55000000000, "US", "Mark Lashier"),
        ("corp_VLO", "Valero Energy", "VLO", "Energy", 50000000000, "US", "Lane Riggs"),
        ("corp_OXY", "Occidental Petroleum", "OXY", "Energy", 50000000000, "US", "Vicki Hollub"),
        ("corp_HAL", "Halliburton Co", "HAL", "Energy", 30000000000, "US", "Jeff Miller"),
        ("corp_BKR", "Baker Hughes", "BKR", "Energy", 40000000000, "US", "Lorenzo Simonelli"),
        ("corp_DVN", "Devon Energy", "DVN", "Energy", 30000000000, "US", "Rick Muncrief"),
        # ── Healthcare ──
        ("corp_AMGN", "Amgen Inc", "AMGN", "Healthcare", 160000000000, "US", "Robert Bradway"),
        ("corp_GILD", "Gilead Sciences", "GILD", "Healthcare", 110000000000, "US", "Daniel O'Day"),
        ("corp_BIIB", "Biogen Inc", "BIIB", "Healthcare", 25000000000, "US", "Chris Viehbacher"),
        ("corp_MRNA", "Moderna Inc", "MRNA", "Healthcare", 20000000000, "US", "Stephane Bancel"),
        ("corp_A", "Agilent Technologies", "A", "Healthcare", 40000000000, "US", "Padraig McDonnell"),
        ("corp_IQV", "IQVIA Holdings", "IQV", "Healthcare", 45000000000, "US", "Ari Bousbib"),
        ("corp_EW", "Edwards Lifesciences", "EW", "Healthcare", 50000000000, "US", "Bernard Zovighian"),
        ("corp_IDXX", "IDEXX Labs", "IDXX", "Healthcare", 40000000000, "US", "Jay Mazelsky"),
        ("corp_DXCM", "DexCom Inc", "DXCM", "Healthcare", 35000000000, "US", "Kevin Sayer"),
        ("corp_HCA", "HCA Healthcare", "HCA", "Healthcare", 90000000000, "US", "Sam Hazen"),
        ("corp_HUM", "Humana Inc", "HUM", "Healthcare", 40000000000, "US", "Jim Rechtin"),
        ("corp_CNC", "Centene Corp", "CNC", "Healthcare", 40000000000, "US", "Sarah London"),
        ("corp_MCK", "McKesson Corp", "MCK", "Healthcare", 80000000000, "US", "Brian Tyler"),
        ("corp_CAH", "Cardinal Health", "CAH", "Healthcare", 30000000000, "US", "Jason Hollar"),
        ("corp_BSX", "Boston Scientific", "BSX", "Healthcare", 120000000000, "US", "Mike Mahoney"),
        ("corp_BDX", "Becton Dickinson", "BDX", "Healthcare", 65000000000, "US", "Tom Polen"),
        ("corp_BAX", "Baxter Intl", "BAX", "Healthcare", 20000000000, "US", "Jose Almeida"),
        ("corp_GEHC", "GE HealthCare", "GEHC", "Healthcare", 45000000000, "US", "Peter Arduini"),
        # ── Consumer ──
        ("corp_MDLZ", "Mondelez Intl", "MDLZ", "Consumer Staples", 90000000000, "US", "Dirk Van de Put"),
        ("corp_GIS", "General Mills", "GIS", "Consumer Staples", 40000000000, "US", "Jeff Harmening"),
        ("corp_K", "Kellanova", "K", "Consumer Staples", 30000000000, "US", "Steve Cahillane"),
        ("corp_KHC", "Kraft Heinz", "KHC", "Consumer Staples", 45000000000, "US", "Carlos Abrams-Rivera"),
        ("corp_HSY", "Hershey Co", "HSY", "Consumer Staples", 35000000000, "US", "Michele Buck"),
        ("corp_SJM", "JM Smucker", "SJM", "Consumer Staples", 15000000000, "US", "Mark Smucker"),
        ("corp_STZ", "Constellation Brands", "STZ", "Consumer Staples", 40000000000, "US", "Bill Newlands"),
        ("corp_DEO", "Diageo plc", "DEO", "Consumer Staples", 70000000000, "UK", "Debra Crew"),
        ("corp_BUD", "Anheuser-Busch InBev", "BUD", "Consumer Staples", 120000000000, "Belgium", "Michel Doukeris"),
        ("corp_EL", "Estee Lauder", "EL", "Consumer Staples", 30000000000, "US", "Stephane de La Faverie"),
        ("corp_AMZN2", "Dollar General", "DG", "Retail", 25000000000, "US", "Todd Vasos"),
        ("corp_DLTR", "Dollar Tree", "DLTR", "Retail", 17000000000, "US", "Mike Creedon"),
        ("corp_ROST", "Ross Stores", "ROST", "Retail", 50000000000, "US", "Barbara Rentler"),
        ("corp_F", "Ford Motor Co", "F", "Automotive", 45000000000, "US", "Jim Farley"),
        ("corp_GM", "General Motors", "GM", "Automotive", 55000000000, "US", "Mary Barra"),
        ("corp_STLA", "Stellantis NV", "STLA", "Automotive", 40000000000, "Netherlands", "Carlos Tavares"),
        ("corp_TM", "Toyota Motor", "TM", "Automotive", 300000000000, "Japan", "Koji Sato"),
        ("corp_HMC", "Honda Motor", "HMC", "Automotive", 55000000000, "Japan", "Toshihiro Mibe"),
        # ── Financial ──
        ("corp_BK", "Bank of NY Mellon", "BK", "Financial", 60000000000, "US", "Robin Vince"),
        ("corp_STT", "State Street Corp", "STT", "Financial", 30000000000, "US", "Ron O'Hanley"),
        ("corp_TFC", "Truist Financial", "TFC", "Financial", 55000000000, "US", "Bill Rogers"),
        ("corp_PNC", "PNC Financial", "PNC", "Financial", 80000000000, "US", "Bill Demchak"),
        ("corp_COF", "Capital One", "COF", "Financial", 65000000000, "US", "Richard Fairbank"),
        ("corp_AIG", "American Intl Group", "AIG", "Financial", 50000000000, "US", "Peter Zaffino"),
        ("corp_MET", "MetLife Inc", "MET", "Financial", 55000000000, "US", "Michel Khalaf"),
        ("corp_PRU", "Prudential Financial", "PRU", "Financial", 45000000000, "US", "Andy Sullivan"),
        ("corp_ALL", "Allstate Corp", "ALL", "Financial", 50000000000, "US", "Tom Wilson"),
        ("corp_TRV", "Travelers Cos", "TRV", "Financial", 55000000000, "US", "Alan Schnitzer"),
        ("corp_MSCI", "MSCI Inc", "MSCI", "Financial", 45000000000, "US", "Henry Fernandez"),
        ("corp_ICE", "Intercontinental Exchange", "ICE", "Financial", 85000000000, "US", "Jeff Sprecher"),
        ("corp_NDAQ", "Nasdaq Inc", "NDAQ", "Financial", 40000000000, "US", "Adena Friedman"),
        # ── Real Estate ──
        ("corp_AMT", "American Tower", "AMT", "Real Estate", 100000000000, "US", "Steven Vondran"),
        ("corp_CCI", "Crown Castle", "CCI", "Real Estate", 45000000000, "US", "Steven Moskowitz"),
        ("corp_EQIX", "Equinix Inc", "EQIX", "Real Estate", 80000000000, "US", "Adaire Fox-Martin"),
        ("corp_PSA", "Public Storage", "PSA", "Real Estate", 55000000000, "US", "Joe Russell"),
        ("corp_WELL", "Welltower Inc", "WELL", "Real Estate", 60000000000, "US", "Shankh Mitra"),
        ("corp_SPG", "Simon Property Group", "SPG", "Real Estate", 55000000000, "US", "David Simon"),
        ("corp_O", "Realty Income", "O", "Real Estate", 55000000000, "US", "Sumit Roy"),
        # ── Utilities ──
        ("corp_D", "Dominion Energy", "D", "Utilities", 50000000000, "US", "Robert Blue"),
        ("corp_AEP", "American Electric Power", "AEP", "Utilities", 55000000000, "US", "Bill Fehrman"),
        ("corp_SRE", "Sempra Energy", "SRE", "Utilities", 55000000000, "US", "Jeff Martin"),
        ("corp_EXC", "Exelon Corp", "EXC", "Utilities", 45000000000, "US", "Calvin Butler"),
        ("corp_XEL", "Xcel Energy", "XEL", "Utilities", 40000000000, "US", "Bob Frenzel"),
        ("corp_CEG", "Constellation Energy", "CEG", "Utilities", 70000000000, "US", "Joe Dominguez"),
        ("corp_VST", "Vistra Corp", "VST", "Utilities", 40000000000, "US", "Jim Burke"),
        # ── Materials ──
        ("corp_APD", "Air Products", "APD", "Materials", 60000000000, "US", "Seifi Ghasemi"),
        ("corp_ECL", "Ecolab Inc", "ECL", "Materials", 60000000000, "US", "Christophe Beck"),
        ("corp_FCX", "Freeport-McMoRan", "FCX", "Materials", 65000000000, "US", "Richard Adkerson"),
        ("corp_NEM", "Newmont Corp", "NEM", "Materials", 55000000000, "US", "Tom Palmer"),
        ("corp_NUE", "Nucor Corp", "NUE", "Materials", 40000000000, "US", "Leon Topalian"),
        ("corp_DOW", "Dow Inc", "DOW", "Materials", 35000000000, "US", "Jim Fitterling"),
        ("corp_DD", "DuPont de Nemours", "DD", "Materials", 35000000000, "US", "Lori Koch"),
        # ── Major International Companies ──
        ("corp_samsung", "Samsung Electronics", "005930.KS", "Technology", 400000000000, "South Korea", "Jay Y. Lee"),
        ("corp_ASML", "ASML Holding", "ASML", "Semiconductors", 350000000000, "Netherlands", "Christophe Fouquet"),
        ("corp_NVO", "Novo Nordisk", "NVO", "Healthcare", 500000000000, "Denmark", "Lars Fruergaard Jorgensen"),
        ("corp_MC", "LVMH", "MC.PA", "Consumer Discretionary", 350000000000, "France", "Bernard Arnault"),
        ("corp_RMS", "Hermes Intl", "RMS.PA", "Consumer Discretionary", 250000000000, "France", "Axel Dumas"),
        ("corp_SAP", "SAP SE", "SAP", "Technology", 280000000000, "Germany", "Christian Klein"),
        ("corp_SIE", "Siemens AG", "SIE.DE", "Industrials", 150000000000, "Germany", "Roland Busch"),
        ("corp_ALV", "Allianz SE", "ALV.DE", "Financial", 120000000000, "Germany", "Oliver Baete"),
        ("corp_DTE", "Deutsche Telekom", "DTE.DE", "Communications", 150000000000, "Germany", "Tim Hottges"),
        ("corp_BAS", "BASF SE", "BAS.DE", "Materials", 45000000000, "Germany", "Markus Kamieth"),
        ("corp_BMW", "BMW AG", "BMW.DE", "Automotive", 60000000000, "Germany", "Oliver Zipse"),
        ("corp_MBG", "Mercedes-Benz Group", "MBG.DE", "Automotive", 70000000000, "Germany", "Ola Kallenius"),
        ("corp_VOW", "Volkswagen AG", "VOW.DE", "Automotive", 60000000000, "Germany", "Oliver Blume"),
        ("corp_AZN", "AstraZeneca", "AZN", "Healthcare", 240000000000, "UK", "Pascal Soriot"),
        ("corp_SHEL", "Shell plc", "SHEL", "Energy", 220000000000, "UK", "Wael Sawan"),
        ("corp_HSBC", "HSBC Holdings", "HSBC", "Financial", 180000000000, "UK", "Noel Quinn"),
        ("corp_BP", "BP plc", "BP", "Energy", 100000000000, "UK", "Murray Auchincloss"),
        ("corp_UL", "Unilever plc", "UL", "Consumer Staples", 150000000000, "UK", "Hein Schumacher"),
        ("corp_RIO", "Rio Tinto", "RIO", "Materials", 110000000000, "UK", "Jakob Stausholm"),
        ("corp_BHP", "BHP Group", "BHP", "Materials", 150000000000, "Australia", "Mike Henry"),
        ("corp_NESN", "Nestle SA", "NESN.SW", "Consumer Staples", 250000000000, "Switzerland", "Laurent Freixe"),
        ("corp_ROG", "Roche Holding", "ROG.SW", "Healthcare", 220000000000, "Switzerland", "Thomas Schinecker"),
        ("corp_NOVN", "Novartis AG", "NOVN.SW", "Healthcare", 230000000000, "Switzerland", "Vas Narasimhan"),
        ("corp_ZURN", "Zurich Insurance", "ZURN.SW", "Financial", 75000000000, "Switzerland", "Mario Greco"),
        ("corp_ABB", "ABB Ltd", "ABB", "Industrials", 100000000000, "Switzerland", "Bjorn Rosengren"),
        ("corp_RELIANCE", "Reliance Industries", "RELIANCE.NS", "Energy", 200000000000, "India", "Mukesh Ambani"),
        ("corp_TCS", "Tata Consultancy", "TCS.NS", "Technology", 160000000000, "India", "K. Krithivasan"),
        ("corp_INFY", "Infosys Ltd", "INFY", "Technology", 80000000000, "India", "Salil Parekh"),
        ("corp_HDFCBANK", "HDFC Bank", "HDB", "Financial", 140000000000, "India", "Sashidhar Jagdishan"),
        ("corp_ICICIBANK", "ICICI Bank", "IBN", "Financial", 95000000000, "India", "Sandeep Bakhshi"),
        ("corp_BHARTI", "Bharti Airtel", "BHARTIARTL.NS", "Communications", 100000000000, "India", "Gopal Vittal"),
        ("corp_ITC", "ITC Ltd", "ITC.NS", "Consumer Staples", 70000000000, "India", "Sanjiv Puri"),
        ("corp_SBIN", "State Bank of India", "SBIN.NS", "Financial", 80000000000, "India", "Dinesh Khara"),
        ("corp_BABA", "Alibaba Group", "BABA", "Technology", 200000000000, "China", "Eddie Wu"),
        ("corp_TCEHY", "Tencent Holdings", "TCEHY", "Technology", 450000000000, "China", "Ma Huateng"),
        ("corp_PDD", "PDD Holdings", "PDD", "Technology", 150000000000, "China", "Chen Lei"),
        ("corp_JD", "JD.com Inc", "JD", "Technology", 50000000000, "China", "Xu Ran"),
        ("corp_BIDU", "Baidu Inc", "BIDU", "Technology", 35000000000, "China", "Robin Li"),
        ("corp_NIO", "NIO Inc", "NIO", "Automotive", 10000000000, "China", "William Li"),
        ("corp_XPEV", "XPeng Inc", "XPEV", "Automotive", 15000000000, "China", "He Xiaopeng"),
        ("corp_LI", "Li Auto Inc", "LI", "Automotive", 25000000000, "China", "Li Xiang"),
        ("corp_MEITUAN", "Meituan", "3690.HK", "Technology", 100000000000, "China", "Wang Xing"),
        ("corp_BYD", "BYD Company", "BYDDY", "Automotive", 100000000000, "China", "Wang Chuanfu"),
        ("corp_CATL", "CATL", "300750.SZ", "Technology", 150000000000, "China", "Zeng Yuqun"),
        ("corp_KWEICHOW", "Kweichow Moutai", "600519.SS", "Consumer Staples", 250000000000, "China", "Ding Xiongjun"),
        ("corp_ICBC", "ICBC", "1398.HK", "Financial", 200000000000, "China", "Liao Lin"),
        ("corp_CCB", "China Construction Bank", "0939.HK", "Financial", 150000000000, "China", "Zhang Jinliang"),
        ("corp_PING_AN", "Ping An Insurance", "2318.HK", "Financial", 100000000000, "China", "Ma Mingzhe"),
        ("corp_CMB", "China Merchants Bank", "3968.HK", "Financial", 100000000000, "China", "Wang Liang"),
        ("corp_SE", "Sea Ltd", "SE", "Technology", 30000000000, "Singapore", "Forrest Li"),
        ("corp_GRAB", "Grab Holdings", "GRAB", "Technology", 15000000000, "Singapore", "Anthony Tan"),
        ("corp_7203", "Toyota Motor", "7203.T", "Automotive", 300000000000, "Japan", "Koji Sato"),
        ("corp_6758", "Sony Group", "SONY", "Technology", 120000000000, "Japan", "Hiroki Totoki"),
        ("corp_9984", "SoftBank Group", "9984.T", "Technology", 100000000000, "Japan", "Masayoshi Son"),
        ("corp_6861", "Keyence Corp", "6861.T", "Technology", 130000000000, "Japan", "Takemitsu Takizaki"),
        ("corp_9983", "Fast Retailing", "9983.T", "Retail", 90000000000, "Japan", "Tadashi Yanai"),
        ("corp_8306", "Mitsubishi UFJ", "MUFG", "Financial", 120000000000, "Japan", "Hironori Kamezawa"),
        ("corp_4063", "Shin-Etsu Chemical", "4063.T", "Materials", 80000000000, "Japan", "Yasuhiko Saitoh"),
        ("corp_6902", "Denso Corp", "6902.T", "Automotive", 40000000000, "Japan", "Koji Arima"),
        ("corp_4568", "Daiichi Sankyo", "4568.T", "Healthcare", 100000000000, "Japan", "Sunao Manabe"),
        ("corp_7741", "HOYA Corp", "7741.T", "Healthcare", 50000000000, "Japan", "Eiichi Katayama"),
        ("corp_SAN", "Banco Santander", "SAN", "Financial", 90000000000, "Spain", "Ana Botin"),
        ("corp_IBE", "Iberdrola", "IBE.MC", "Utilities", 80000000000, "Spain", "Jose Ignacio Sanchez Galan"),
        ("corp_ITX", "Inditex", "ITX.MC", "Retail", 140000000000, "Spain", "Oscar Garcia Maceiras"),
        ("corp_ENEL", "Enel SpA", "ENEL.MI", "Utilities", 70000000000, "Italy", "Flavio Cattaneo"),
        ("corp_ISP", "Intesa Sanpaolo", "ISP.MI", "Financial", 70000000000, "Italy", "Carlo Messina"),
        ("corp_UCG", "UniCredit SpA", "UCG.MI", "Financial", 65000000000, "Italy", "Andrea Orcel"),
        ("corp_FER", "Ferrari NV", "RACE", "Automotive", 80000000000, "Italy", "Benedetto Vigna"),
        ("corp_OR", "L'Oreal", "OR.PA", "Consumer Staples", 230000000000, "France", "Nicolas Hieronimus"),
        ("corp_TTE", "TotalEnergies", "TTE", "Energy", 150000000000, "France", "Patrick Pouyanne"),
        ("corp_SU", "Schneider Electric", "SU.PA", "Industrials", 120000000000, "France", "Peter Herweck"),
        ("corp_AI", "Air Liquide", "AI.PA", "Materials", 90000000000, "France", "Francois Jackow"),
        ("corp_BNP", "BNP Paribas", "BNP.PA", "Financial", 80000000000, "France", "Jean-Laurent Bonnafe"),
        ("corp_SAN_FR", "Sanofi SA", "SNY", "Healthcare", 130000000000, "France", "Paul Hudson"),
        ("corp_ABI", "AB InBev", "ABI.BR", "Consumer Staples", 120000000000, "Belgium", "Michel Doukeris"),
        ("corp_NOVO", "Novo Nordisk", "NOVO-B.CO", "Healthcare", 500000000000, "Denmark", "Lars Fruergaard Jorgensen"),
        ("corp_MAERSK", "AP Moller-Maersk", "MAERSK-B.CO", "Industrials", 30000000000, "Denmark", "Vincent Clerc"),
        ("corp_NESTE", "Neste Oyj", "NESTE.HE", "Energy", 20000000000, "Finland", "Matti Lehmus"),
        ("corp_NOKIA", "Nokia Corp", "NOK", "Technology", 25000000000, "Finland", "Pekka Lundmark"),
        ("corp_ERIC", "Ericsson", "ERIC", "Technology", 30000000000, "Sweden", "Borje Ekholm"),
        ("corp_VOLVO", "Volvo Group", "VOLV-B.ST", "Industrials", 50000000000, "Sweden", "Martin Lundstedt"),
        ("corp_ATLAS", "Atlas Copco", "ATCO-A.ST", "Industrials", 70000000000, "Sweden", "Vagn Sorensen"),
        ("corp_EQUINOR", "Equinor ASA", "EQNR", "Energy", 70000000000, "Norway", "Anders Opedal"),
        ("corp_VALE", "Vale SA", "VALE", "Materials", 55000000000, "Brazil", "Eduardo Bartolomeo"),
        ("corp_ITUB", "Itau Unibanco", "ITUB", "Financial", 60000000000, "Brazil", "Milton Maluhy Filho"),
        ("corp_PBR", "Petrobras", "PBR", "Energy", 100000000000, "Brazil", "Jean Paul Prates"),
        ("corp_NU", "Nu Holdings", "NU", "Financial", 55000000000, "Brazil", "David Velez"),
        ("corp_AMX", "America Movil", "AMX", "Communications", 60000000000, "Mexico", "Daniel Hajj Aboumrad"),
        ("corp_WALMEX", "Walmart de Mexico", "WALMEX.MX", "Retail", 50000000000, "Mexico", "Guilherme Loureiro"),
        ("corp_AC", "Arca Continental", "AC.MX", "Consumer Staples", 15000000000, "Mexico", "Arturo Gutierrez Hernandez"),
        ("corp_ARAMCO", "Saudi Aramco", "2222.SR", "Energy", 1800000000000, "Saudi Arabia", "Amin H. Nasser"),
        ("corp_STC", "Saudi Telecom", "7010.SR", "Communications", 50000000000, "Saudi Arabia", "Olayan Alwetaid"),
        ("corp_QNB", "QNB Group", "QNB", "Financial", 45000000000, "Qatar", "Abdulla Mubarak Al-Khalifa"),
        ("corp_FAB", "First Abu Dhabi Bank", "FAB.AD", "Financial", 45000000000, "UAE", "Hana Al Rostamani"),
        ("corp_ADNOC", "ADNOC Distribution", "ADNOCDIST.AD", "Energy", 10000000000, "UAE", "Bader Al Lamki"),
        ("corp_NASPERS", "Naspers Ltd", "NPN.JO", "Technology", 30000000000, "South Africa", "Phuthi Mahanyele-Dabengwa"),
        ("corp_MTN", "MTN Group", "MTN.JO", "Communications", 10000000000, "South Africa", "Ralph Mupita"),
        ("corp_FNB", "FirstRand Ltd", "FSR.JO", "Financial", 25000000000, "South Africa", "Mary Memory Gillfillan"),
        ("corp_DANGOTE_CEMENT", "Dangote Cement", "DANGCEM.LG", "Materials", 10000000000, "Nigeria", "Arvind Pathak"),
        ("corp_CBA", "Commonwealth Bank", "CBA.AX", "Financial", 150000000000, "Australia", "Matt Comyn"),
        ("corp_CSL", "CSL Ltd", "CSL.AX", "Healthcare", 100000000000, "Australia", "Paul McKenzie"),
        ("corp_FMG", "Fortescue Metals", "FMG.AX", "Materials", 50000000000, "Australia", "Dino Otranto"),
        ("corp_WDS", "Woodside Energy", "WDS.AX", "Energy", 30000000000, "Australia", "Meg O'Neill"),
        ("corp_RY", "Royal Bank of Canada", "RY", "Financial", 170000000000, "Canada", "Dave McKay"),
        ("corp_TD", "Toronto-Dominion", "TD", "Financial", 120000000000, "Canada", "Bharat Masrani"),
        ("corp_ENB", "Enbridge Inc", "ENB", "Energy", 85000000000, "Canada", "Greg Ebel"),
        ("corp_CNR", "Canadian National Railway", "CNI", "Industrials", 75000000000, "Canada", "Tracy Robinson"),
        ("corp_BMO", "Bank of Montreal", "BMO", "Financial", 70000000000, "Canada", "Darryl White"),
        ("corp_BNS", "Bank of Nova Scotia", "BNS", "Financial", 65000000000, "Canada", "Scott Thomson"),
        ("corp_CP", "Canadian Pacific Kansas City", "CP", "Industrials", 70000000000, "Canada", "Keith Creel"),
        # ── Additional S&P 500 and global companies ──
        ("corp_FTNT", "Fortinet Inc", "FTNT", "Technology", 65000000000, "US", "Ken Xie"),
        ("corp_MNST", "Monster Beverage", "MNST", "Consumer Staples", 55000000000, "US", "Hilton Schlosberg"),
        ("corp_KLAC2", "KLA Corp", "KLAC", "Semiconductors", 100000000000, "US", "Rick Wallace"),
        ("corp_ON", "ON Semiconductor", "ON", "Semiconductors", 30000000000, "US", "Hassane El-Khoury"),
        ("corp_NXPI", "NXP Semiconductors", "NXPI", "Semiconductors", 55000000000, "Netherlands", "Kurt Sievers"),
        ("corp_MCHP", "Microchip Technology", "MCHP", "Semiconductors", 35000000000, "US", "Ganesh Moorthy"),
        ("corp_SWKS", "Skyworks Solutions", "SWKS", "Semiconductors", 15000000000, "US", "Liam Griffin"),
        ("corp_MPWR", "Monolithic Power Systems", "MPWR", "Semiconductors", 40000000000, "US", "Michael Hsing"),
        ("corp_GWW", "W.W. Grainger", "GWW", "Industrials", 55000000000, "US", "DJ Mackie"),
        ("corp_ECL2", "Ecolab Inc", "ECL", "Materials", 60000000000, "US", "Christophe Beck"),
        ("corp_VRSK", "Verisk Analytics", "VRSK", "Technology", 40000000000, "US", "Lee Shavel"),
        ("corp_FICO", "Fair Isaac Corp", "FICO", "Technology", 50000000000, "US", "Will Lansing"),
        ("corp_MSCI2", "MSCI Inc", "MSCI", "Financial", 45000000000, "US", "Henry Fernandez"),
        ("corp_AXON", "Axon Enterprise", "AXON", "Technology", 40000000000, "US", "Rick Smith"),
        ("corp_WST", "West Pharmaceutical", "WST", "Healthcare", 25000000000, "US", "Eric Green"),
        ("corp_TECH", "Bio-Techne", "TECH", "Healthcare", 10000000000, "US", "Kim Kelderman"),
        ("corp_PODD", "Insulet Corp", "PODD", "Healthcare", 20000000000, "US", "Jim Hollingshead"),
        ("corp_TDG", "TransDigm Group", "TDG", "Industrials", 75000000000, "US", "Kevin Stein"),
        ("corp_HWM", "Howmet Aerospace", "HWM", "Industrials", 45000000000, "US", "John Plant"),
        ("corp_WAB", "Wabtec Corp", "WAB", "Industrials", 25000000000, "US", "Rafael Santana"),
        ("corp_IR", "Ingersoll Rand", "IR", "Industrials", 40000000000, "US", "Vicente Reynal"),
        ("corp_TRGP", "Targa Resources", "TRGP", "Energy", 35000000000, "US", "Matt Meloy"),
        ("corp_OKE", "ONEOK Inc", "OKE", "Energy", 55000000000, "US", "Pierce Norton"),
        ("corp_WMB", "Williams Companies", "WMB", "Energy", 55000000000, "US", "Alan Armstrong"),
        ("corp_KMI", "Kinder Morgan", "KMI", "Energy", 50000000000, "US", "Kim Dang"),
        ("corp_ET", "Energy Transfer", "ET", "Energy", 55000000000, "US", "Tom Long"),
        ("corp_EPD", "Enterprise Products", "EPD", "Energy", 65000000000, "US", "Jim Teague"),
        ("corp_FANG", "Diamondback Energy", "FANG", "Energy", 50000000000, "US", "Travis Stice"),
        ("corp_APA", "APA Corp", "APA", "Energy", 10000000000, "US", "John Christmann"),
        ("corp_HES", "Hess Corp", "HES", "Energy", 45000000000, "US", "John Hess"),
        ("corp_MRO", "Marathon Oil", "MRO", "Energy", 15000000000, "US", "Lee Tillman"),
        ("corp_TRMB", "Trimble Inc", "TRMB", "Technology", 17000000000, "US", "Rob Painter"),
        ("corp_ZBRA", "Zebra Technologies", "ZBRA", "Technology", 18000000000, "US", "Bill Burns"),
        ("corp_KEYS", "Keysight Technologies", "KEYS", "Technology", 30000000000, "US", "Satish Dhanasekaran"),
        ("corp_ANSS", "ANSYS Inc", "ANSS", "Technology", 30000000000, "US", "Ajei Gopal"),
        ("corp_CDW", "CDW Corp", "CDW", "Technology", 25000000000, "US", "Christine Leahy"),
        ("corp_GDDY", "GoDaddy Inc", "GDDY", "Technology", 25000000000, "US", "Aman Bhutani"),
        ("corp_GEN", "Gen Digital", "GEN", "Technology", 17000000000, "US", "Vincent Pilette"),
        ("corp_VRSN", "VeriSign Inc", "VRSN", "Technology", 22000000000, "US", "Jim Bidzos"),
        ("corp_AKAM", "Akamai Technologies", "AKAM", "Technology", 15000000000, "US", "Tom Leighton"),
        ("corp_WDC", "Western Digital", "WDC", "Technology", 20000000000, "US", "David Goeckeler"),
        ("corp_STX", "Seagate Technology", "STX", "Technology", 20000000000, "US", "Dave Mosley"),
        ("corp_HPQ", "HP Inc", "HPQ", "Technology", 35000000000, "US", "Enrique Lores"),
        ("corp_HPE", "HP Enterprise", "HPE", "Technology", 25000000000, "US", "Antonio Neri"),
        ("corp_DELL", "Dell Technologies", "DELL", "Technology", 80000000000, "US", "Michael Dell"),
        ("corp_NTAP", "NetApp Inc", "NTAP", "Technology", 22000000000, "US", "George Kurian"),
        ("corp_JNPR", "Juniper Networks", "JNPR", "Technology", 14000000000, "US", "Rami Rahim"),
        ("corp_FFIV", "F5 Inc", "FFIV", "Technology", 12000000000, "US", "Francois Locoh-Donou"),
        ("corp_CTSH", "Cognizant Technology", "CTSH", "Technology", 40000000000, "US", "Ravi Kumar"),
        ("corp_IT", "Gartner Inc", "IT", "Technology", 40000000000, "US", "Gene Hall"),
        ("corp_BR", "Broadridge Financial", "BR", "Technology", 25000000000, "US", "Tim Gokey"),
        ("corp_LDOS", "Leidos Holdings", "LDOS", "Technology", 20000000000, "US", "Tom Bell"),
        ("corp_SAIC", "Science Applications", "SAIC", "Technology", 8000000000, "US", "Toni Townes-Whitley"),
        ("corp_EPAM", "EPAM Systems", "EPAM", "Technology", 12000000000, "US", "Arkadiy Dobkin"),
        ("corp_PAYC", "Paycom Software", "PAYC", "Technology", 12000000000, "US", "Chad Richison"),
        ("corp_PCTY", "Paylocity Holding", "PCTY", "Technology", 10000000000, "US", "Steve Beauchamp"),
        ("corp_HUBS", "HubSpot Inc", "HUBS", "Technology", 30000000000, "US", "Yamini Rangan"),
        ("corp_BILL", "BILL Holdings", "BILL", "Technology", 8000000000, "US", "Rene Lacerte"),
        ("corp_VEEV", "Veeva Systems", "VEEV", "Technology", 35000000000, "US", "Peter Gassner"),
        ("corp_SPLK", "Splunk Inc", "SPLK", "Technology", 25000000000, "US", "Gary Steele"),
        ("corp_MDB", "MongoDB Inc", "MDB", "Technology", 20000000000, "US", "Dev Ittycheria"),
        ("corp_ESTC", "Elastic NV", "ESTC", "Technology", 10000000000, "US", "Ash Kulkarni"),
        ("corp_CFLT", "Confluent Inc", "CFLT", "Technology", 10000000000, "US", "Jay Kreps"),
        ("corp_DOCN", "DigitalOcean", "DOCN", "Technology", 4000000000, "US", "Paddy Srinivasan"),
        ("corp_S", "SentinelOne", "S", "Technology", 8000000000, "US", "Tomer Weingarten"),
        ("corp_LULU", "Lululemon Athletica", "LULU", "Consumer Discretionary", 40000000000, "Canada", "Calvin McDonald"),
        ("corp_DECK", "Deckers Outdoor", "DECK", "Consumer Discretionary", 25000000000, "US", "Dave Powers"),
        ("corp_BIRK", "Birkenstock Holding", "BIRK", "Consumer Discretionary", 10000000000, "Germany", "Oliver Reichert"),
        ("corp_TPR", "Tapestry Inc", "TPR", "Consumer Discretionary", 15000000000, "US", "Joanne Crevoiserat"),
        ("corp_RL", "Ralph Lauren", "RL", "Consumer Discretionary", 12000000000, "US", "Patrice Louvet"),
        ("corp_CPRI", "Capri Holdings", "CPRI", "Consumer Discretionary", 7000000000, "UK", "John Idol"),
        ("corp_PVH", "PVH Corp", "PVH", "Consumer Discretionary", 6000000000, "US", "Stefan Larsson"),
        ("corp_HLT", "Hilton Worldwide", "HLT", "Consumer Discretionary", 55000000000, "US", "Chris Nassetta"),
        ("corp_MAR", "Marriott Intl", "MAR", "Consumer Discretionary", 75000000000, "US", "Anthony Capuano"),
        ("corp_H", "Hyatt Hotels", "H", "Consumer Discretionary", 15000000000, "US", "Mark Hoplamazian"),
        ("corp_RCL", "Royal Caribbean", "RCL", "Consumer Discretionary", 55000000000, "US", "Jason Liberty"),
        ("corp_CCL", "Carnival Corp", "CCL", "Consumer Discretionary", 25000000000, "US", "Josh Weinstein"),
        ("corp_NCLH", "Norwegian Cruise Line", "NCLH", "Consumer Discretionary", 10000000000, "US", "Harry Sommer"),
        ("corp_LVS", "Las Vegas Sands", "LVS", "Consumer Discretionary", 40000000000, "US", "Rob Goldstein"),
        ("corp_MGM", "MGM Resorts", "MGM", "Consumer Discretionary", 12000000000, "US", "Bill Hornbuckle"),
        ("corp_WYNN", "Wynn Resorts", "WYNN", "Consumer Discretionary", 10000000000, "US", "Craig Billings"),
        ("corp_DKS", "Dick's Sporting Goods", "DKS", "Retail", 15000000000, "US", "Lauren Hobart"),
        ("corp_BBY", "Best Buy Co", "BBY", "Retail", 20000000000, "US", "Corie Barry"),
        ("corp_AZO", "AutoZone Inc", "AZO", "Retail", 55000000000, "US", "Phil Daniele"),
        ("corp_AAP", "Advance Auto Parts", "AAP", "Retail", 4000000000, "US", "Shane O'Kelly"),
        ("corp_KR", "Kroger Co", "KR", "Retail", 40000000000, "US", "Rodney McMullen"),
        ("corp_SYY", "Sysco Corp", "SYY", "Retail", 40000000000, "US", "Kevin Hourican"),
        ("corp_EBAY", "eBay Inc", "EBAY", "Technology", 30000000000, "US", "Jamie Iannone"),
        ("corp_ETSY", "Etsy Inc", "ETSY", "Technology", 7000000000, "US", "Josh Silverman"),
        ("corp_W", "Wayfair Inc", "W", "Technology", 7000000000, "US", "Niraj Shah"),
        ("corp_CHWY", "Chewy Inc", "CHWY", "Retail", 12000000000, "US", "Sumit Singh"),
        ("corp_ZM", "Zoom Video Communications", "ZM", "Technology", 22000000000, "US", "Eric Yuan"),
        ("corp_DOCU", "DocuSign Inc", "DOCU", "Technology", 15000000000, "US", "Allan Thygesen"),
        ("corp_RNG", "RingCentral", "RNG", "Technology", 5000000000, "US", "Vlad Shmunis"),
        ("corp_TOST", "Toast Inc", "TOST", "Technology", 15000000000, "US", "Aman Narang"),
        ("corp_FIVN", "Five9 Inc", "FIVN", "Technology", 4000000000, "US", "Mike Burkland"),
        ("corp_APP", "AppLovin Corp", "APP", "Technology", 100000000000, "US", "Adam Foroughi"),
        ("corp_DUOL", "Duolingo Inc", "DUOL", "Technology", 12000000000, "US", "Luis von Ahn"),
        ("corp_SMCI", "Super Micro Computer", "SMCI", "Technology", 25000000000, "US", "Charles Liang"),
        ("corp_ARM", "Arm Holdings", "ARM", "Semiconductors", 150000000000, "UK", "Rene Haas"),
        ("corp_GFS", "GlobalFoundries", "GFS", "Semiconductors", 25000000000, "US", "Thomas Caulfield"),
        ("corp_WOLF", "Wolfspeed Inc", "WOLF", "Semiconductors", 2000000000, "US", "Gregg Lowe"),
        ("corp_ENPH", "Enphase Energy", "ENPH", "Technology", 10000000000, "US", "Badri Kothandaraman"),
        ("corp_SEDG", "SolarEdge Technologies", "SEDG", "Technology", 3000000000, "Israel", "Zvi Lando"),
        ("corp_FSLR", "First Solar Inc", "FSLR", "Technology", 20000000000, "US", "Mark Widmar"),
        ("corp_RUN", "Sunrun Inc", "RUN", "Utilities", 4000000000, "US", "Mary Powell"),
        ("corp_PLUG", "Plug Power", "PLUG", "Industrials", 2000000000, "US", "Andy Marsh"),
        ("corp_BLDR", "Builders FirstSource", "BLDR", "Industrials", 22000000000, "US", "Dave Rush"),
        ("corp_PWR", "Quanta Services", "PWR", "Industrials", 45000000000, "US", "Earl Austin"),
        ("corp_EME", "EMCOR Group", "EME", "Industrials", 20000000000, "US", "Tony Guzzi"),
        ("corp_J", "Jacobs Solutions", "J", "Industrials", 18000000000, "US", "Bob Pragada"),
        ("corp_ACM", "AECOM", "ACM", "Industrials", 15000000000, "US", "Troy Rudd"),
        ("corp_AME", "AMETEK Inc", "AME", "Industrials", 40000000000, "US", "David Zapico"),
        ("corp_HUBB", "Hubbell Inc", "HUBB", "Industrials", 20000000000, "US", "Gerben Bakker"),
        ("corp_DOV", "Dover Corp", "DOV", "Industrials", 25000000000, "US", "Rich Tobin"),
        ("corp_XYL", "Xylem Inc", "XYL", "Industrials", 28000000000, "US", "Matthew Pine"),
        ("corp_IEX", "IDEX Corp", "IEX", "Industrials", 15000000000, "US", "Eric Ashleman"),
        ("corp_GNRC", "Generac Holdings", "GNRC", "Industrials", 10000000000, "US", "Aaron Jagdfeld"),
        ("corp_WSO", "Watsco Inc", "WSO", "Industrials", 20000000000, "US", "Albert Nahmad"),
        ("corp_STE", "STERIS plc", "STE", "Healthcare", 22000000000, "US", "Dan Carestio"),
        ("corp_HOLX", "Hologic Inc", "HOLX", "Healthcare", 20000000000, "US", "Steve MacMillan"),
        ("corp_TFX", "Teleflex Inc", "TFX", "Healthcare", 10000000000, "US", "Liam Kelly"),
        ("corp_RMD", "ResMed Inc", "RMD", "Healthcare", 35000000000, "US", "Mick Farrell"),
        ("corp_ALGN", "Align Technology", "ALGN", "Healthcare", 18000000000, "US", "Joe Hogan"),
        ("corp_INCY", "Incyte Corp", "INCY", "Healthcare", 15000000000, "US", "Herve Hoppenot"),
        ("corp_ALNY", "Alnylam Pharmaceuticals", "ALNY", "Healthcare", 30000000000, "US", "Yvonne Greenstreet"),
        ("corp_BMRN", "BioMarin Pharmaceutical", "BMRN", "Healthcare", 15000000000, "US", "Alexander Hardy"),
        ("corp_SGEN", "Seagen Inc", "SGEN", "Healthcare", 40000000000, "US", "David Epstein"),
        ("corp_EXAS", "Exact Sciences", "EXAS", "Healthcare", 10000000000, "US", "Kevin Conroy"),
        ("corp_NBIX", "Neurocrine Biosciences", "NBIX", "Healthcare", 15000000000, "US", "Kevin Gorman"),
        ("corp_PCVX", "Vaxcyte Inc", "PCVX", "Healthcare", 12000000000, "US", "Grant Pickering"),
        ("corp_ARGX", "argenx SE", "ARGX", "Healthcare", 30000000000, "Netherlands", "Tim Van Hauwermeiren"),
        ("corp_AFL", "Aflac Inc", "AFL", "Financial", 55000000000, "US", "Dan Amos"),
        ("corp_AON", "Aon plc", "AON", "Financial", 75000000000, "Ireland", "Greg Case"),
        ("corp_WTW", "Willis Towers Watson", "WTW", "Financial", 30000000000, "UK", "Carl Hess"),
        ("corp_CINF", "Cincinnati Financial", "CINF", "Financial", 20000000000, "US", "Steve Johnston"),
        ("corp_HIG", "Hartford Financial", "HIG", "Financial", 30000000000, "US", "Chris Swift"),
        ("corp_FITB", "Fifth Third Bancorp", "FITB", "Financial", 30000000000, "US", "Tim Spence"),
        ("corp_RF", "Regions Financial", "RF", "Financial", 22000000000, "US", "John Turner"),
        ("corp_KEY", "KeyCorp", "KEY", "Financial", 16000000000, "US", "Chris Gorman"),
        ("corp_MTB", "M&T Bank Corp", "MTB", "Financial", 30000000000, "US", "Rene Jones"),
        ("corp_CFG", "Citizens Financial", "CFG", "Financial", 18000000000, "US", "Bruce Van Saun"),
        ("corp_HBAN", "Huntington Bancshares", "HBAN", "Financial", 22000000000, "US", "Steve Steinour"),
        ("corp_ZION", "Zions Bancorporation", "ZION", "Financial", 8000000000, "US", "Harris Simmons"),
        ("corp_FRC2", "First Republic (now JPM)", "FRC", "Financial", 0, "US", "N/A"),
        ("corp_SIVB2", "SVB Financial (now FCNCA)", "SIVB", "Financial", 0, "US", "N/A"),
        ("corp_CMA", "Comerica Inc", "CMA", "Financial", 8000000000, "US", "Curt Farmer"),
        ("corp_ALLY", "Ally Financial", "ALLY", "Financial", 12000000000, "US", "Michael Rhodes"),
        ("corp_DFS", "Discover Financial", "DFS", "Financial", 40000000000, "US", "Michael Rhodes"),
        ("corp_SYF", "Synchrony Financial", "SYF", "Financial", 20000000000, "US", "Brian Doubles"),
        ("corp_RJF", "Raymond James", "RJF", "Financial", 30000000000, "US", "Paul Reilly"),
        ("corp_IBKR", "Interactive Brokers", "IBKR", "Financial", 55000000000, "US", "Milan Galik"),
        ("corp_LPLA", "LPL Financial", "LPLA", "Financial", 25000000000, "US", "Dan Arnold"),
        ("corp_MKTX", "MarketAxess Holdings", "MKTX", "Financial", 10000000000, "US", "Chris Concannon"),
        ("corp_CBOE", "Cboe Global Markets", "CBOE", "Financial", 20000000000, "US", "Fred Tomczyk"),
        ("corp_MO", "Altria Group", "MO", "Consumer Staples", 90000000000, "US", "Billy Gifford"),
        ("corp_TAP", "Molson Coors", "TAP", "Consumer Staples", 15000000000, "US", "Gavin Hattersley"),
        ("corp_SAM", "Boston Beer Co", "SAM", "Consumer Staples", 5000000000, "US", "Michael Spillane"),
        ("corp_MNST2", "Monster Beverage", "MNST", "Consumer Staples", 55000000000, "US", "Hilton Schlosberg"),
        ("corp_CLX", "Clorox Co", "CLX", "Consumer Staples", 20000000000, "US", "Linda Rendle"),
        ("corp_CHD", "Church & Dwight", "CHD", "Consumer Staples", 25000000000, "US", "Matt Farrell"),
        ("corp_KMB", "Kimberly-Clark", "KMB", "Consumer Staples", 45000000000, "US", "Mike Hsu"),
        ("corp_SPC", "Spectrum Brands", "SPB", "Consumer Staples", 3000000000, "US", "David Maura"),
        ("corp_HRL", "Hormel Foods", "HRL", "Consumer Staples", 18000000000, "US", "Jim Snee"),
        ("corp_CART", "Instacart (Maplebear)", "CART", "Technology", 10000000000, "US", "Fidji Simo"),
        ("corp_HOOD", "Robinhood Markets", "HOOD", "Financial", 30000000000, "US", "Vlad Tenev"),
        ("corp_SOFI", "SoFi Technologies", "SOFI", "Financial", 12000000000, "US", "Anthony Noto"),
        ("corp_AFRM", "Affirm Holdings", "AFRM", "Financial", 15000000000, "US", "Max Levchin"),
        ("corp_UPST", "Upstart Holdings", "UPST", "Financial", 5000000000, "US", "Dave Girouard"),
        ("corp_FOUR", "Shift4 Payments", "FOUR", "Technology", 8000000000, "US", "Jared Isaacman"),
        ("corp_ADP2", "Automatic Data Processing", "ADP", "Technology", 120000000000, "US", "Maria Black"),
        ("corp_PAYX", "Paychex Inc", "PAYX", "Technology", 50000000000, "US", "John Gibson"),
        ("corp_CPAY", "Corpay Inc", "CPAY", "Technology", 25000000000, "US", "Ron Clarke"),
        ("corp_WEX", "WEX Inc", "WEX", "Technology", 8000000000, "US", "Melissa Smith"),
        ("corp_GPN", "Global Payments", "GPN", "Technology", 25000000000, "US", "Cameron Bready"),
        ("corp_JKHY", "Jack Henry & Associates", "JKHY", "Technology", 13000000000, "US", "Greg Adelson"),
        # ── More international companies ──
        ("corp_GLEN", "Glencore plc", "GLEN.L", "Materials", 60000000000, "Switzerland", "Gary Nagle"),
        ("corp_UBS", "UBS Group", "UBS", "Financial", 100000000000, "Switzerland", "Sergio Ermotti"),
        ("corp_CSGN", "Credit Suisse (now UBS)", "CSGN", "Financial", 0, "Switzerland", "N/A"),
        ("corp_NESTLE2", "Nestle SA", "NESN.SW", "Consumer Staples", 250000000000, "Switzerland", "Laurent Freixe"),
        ("corp_GIVN", "Givaudan SA", "GIVN.SW", "Materials", 40000000000, "Switzerland", "Gilles Andrier"),
        ("corp_SIKA", "Sika AG", "SIKA.SW", "Materials", 45000000000, "Switzerland", "Thomas Hasler"),
        ("corp_LONN", "Lonza Group", "LONN.SW", "Healthcare", 40000000000, "Switzerland", "Wolfgang Wienand"),
        ("corp_BARC", "Barclays plc", "BARC.L", "Financial", 45000000000, "UK", "C.S. Venkatakrishnan"),
        ("corp_LLOY", "Lloyds Banking Group", "LLOY.L", "Financial", 50000000000, "UK", "Charlie Nunn"),
        ("corp_NWG", "NatWest Group", "NWG.L", "Financial", 35000000000, "UK", "Paul Thwaite"),
        ("corp_GSK", "GSK plc", "GSK", "Healthcare", 80000000000, "UK", "Emma Walmsley"),
        ("corp_DGE", "Diageo plc", "DGE.L", "Consumer Staples", 70000000000, "UK", "Debra Crew"),
        ("corp_RECKITT", "Reckitt Benckiser", "RKT.L", "Consumer Staples", 40000000000, "UK", "Kris Licht"),
        ("corp_BURBERRY", "Burberry Group", "BRBY.L", "Consumer Discretionary", 10000000000, "UK", "Joshua Schulman"),
        ("corp_BA_L", "BAE Systems", "BA.L", "Industrials", 50000000000, "UK", "Charles Woodburn"),
        ("corp_RR", "Rolls-Royce Holdings", "RR.L", "Industrials", 55000000000, "UK", "Tufan Erginbilgic"),
        ("corp_EXPN", "Experian plc", "EXPN.L", "Financial", 40000000000, "UK", "Brian Cassin"),
        ("corp_LSEG", "London Stock Exchange", "LSEG.L", "Financial", 60000000000, "UK", "David Schwimmer"),
        ("corp_VOD", "Vodafone Group", "VOD.L", "Communications", 25000000000, "UK", "Margherita Della Valle"),
        ("corp_BT", "BT Group", "BT.A.L", "Communications", 15000000000, "UK", "Allison Kirkby"),
        ("corp_TSCO", "Tesco plc", "TSCO.L", "Retail", 30000000000, "UK", "Ken Murphy"),
        ("corp_AAL", "Anglo American", "AAL.L", "Materials", 35000000000, "UK", "Duncan Wanblad"),
        ("corp_ANTO", "Antofagasta", "ANTO.L", "Materials", 20000000000, "UK", "Ivan Arriagada"),
        ("corp_ADIDAS", "Adidas AG", "ADS.DE", "Consumer Discretionary", 40000000000, "Germany", "Bjorn Gulden"),
        ("corp_PUMA", "Puma SE", "PUM.DE", "Consumer Discretionary", 8000000000, "Germany", "Arne Freundt"),
        ("corp_DB1", "Deutsche Boerse", "DB1.DE", "Financial", 40000000000, "Germany", "Theodor Weimer"),
        ("corp_IFX", "Infineon Technologies", "IFX.DE", "Semiconductors", 40000000000, "Germany", "Jochen Hanebeck"),
        ("corp_MRK_DE", "Merck KGaA", "MRK.DE", "Healthcare", 50000000000, "Germany", "Belen Garijo"),
        ("corp_FRE", "Fresenius SE", "FRE.DE", "Healthcare", 15000000000, "Germany", "Michael Sen"),
        ("corp_HEN3", "Henkel AG", "HEN3.DE", "Consumer Staples", 35000000000, "Germany", "Carsten Knobel"),
        ("corp_ADS", "Adidas AG", "ADS.DE", "Consumer Discretionary", 40000000000, "Germany", "Bjorn Gulden"),
        ("corp_LHA", "Lufthansa Group", "LHA.DE", "Industrials", 10000000000, "Germany", "Carsten Spohr"),
        ("corp_ADS_DE", "Delivery Hero", "DHER.DE", "Technology", 8000000000, "Germany", "Niklas Ostberg"),
        ("corp_ZAL", "Zalando SE", "ZAL.DE", "Technology", 8000000000, "Germany", "Robert Gentz"),
        ("corp_EAF", "Airbus SE", "AIR.PA", "Industrials", 130000000000, "France", "Guillaume Faury"),
        ("corp_DG_FR", "Vinci SA", "DG.PA", "Industrials", 75000000000, "France", "Xavier Huillard"),
        ("corp_SGO", "Saint-Gobain", "SGO.PA", "Materials", 45000000000, "France", "Benoit Bazin"),
        ("corp_ORA", "Orange SA", "ORA.PA", "Communications", 30000000000, "France", "Christel Heydemann"),
        ("corp_ACA", "Credit Agricole", "ACA.PA", "Financial", 40000000000, "France", "Philippe Brassac"),
        ("corp_GLE", "Societe Generale", "GLE.PA", "Financial", 25000000000, "France", "Slawomir Krupa"),
        ("corp_CAP", "Capgemini SE", "CAP.PA", "Technology", 35000000000, "France", "Aiman Ezzat"),
        ("corp_DSY", "Dassault Systemes", "DSY.PA", "Technology", 45000000000, "France", "Pascal Daloz"),
        ("corp_SAN_ES", "Banco Santander", "SAN.MC", "Financial", 90000000000, "Spain", "Ana Botin"),
        ("corp_BBVA", "BBVA", "BBVA.MC", "Financial", 60000000000, "Spain", "Onur Genc"),
        ("corp_TEF", "Telefonica SA", "TEF.MC", "Communications", 25000000000, "Spain", "Jose Maria Alvarez-Pallete"),
        ("corp_REP", "Repsol SA", "REP.MC", "Energy", 20000000000, "Spain", "Josu Jon Imaz"),
        ("corp_AMS", "Amadeus IT", "AMS.MC", "Technology", 35000000000, "Spain", "Luis Maroto"),
        ("corp_ASML2", "ASML Holding", "ASML.AS", "Semiconductors", 350000000000, "Netherlands", "Christophe Fouquet"),
        ("corp_PHIA", "Philips NV", "PHIA.AS", "Healthcare", 25000000000, "Netherlands", "Roy Jakobs"),
        ("corp_UNA", "Unilever NV", "UNA.AS", "Consumer Staples", 150000000000, "Netherlands", "Hein Schumacher"),
        ("corp_INGA", "ING Group", "INGA.AS", "Financial", 55000000000, "Netherlands", "Steven van Rijswijk"),
        ("corp_AD", "Ahold Delhaize", "AD.AS", "Retail", 30000000000, "Netherlands", "Frans Muller"),
        ("corp_PROSUS", "Prosus NV", "PRX.AS", "Technology", 50000000000, "Netherlands", "Fabricio Bloisi"),
        ("corp_ADYEN", "Adyen NV", "ADYEN.AS", "Financial", 50000000000, "Netherlands", "Pieter van der Does"),
        ("corp_WKL", "Wolters Kluwer", "WKL.AS", "Technology", 35000000000, "Netherlands", "Nancy McKinstry"),
        ("corp_ERIC_SE", "Ericsson", "ERIC-B.ST", "Technology", 30000000000, "Sweden", "Borje Ekholm"),
        ("corp_HEXA", "Hexagon AB", "HEXA-B.ST", "Technology", 30000000000, "Sweden", "Paolo Guglielmini"),
        ("corp_SEB", "SEB SA", "SEB.PA", "Consumer Discretionary", 8000000000, "France", "Stanislas de Gramont"),
        ("corp_NOVO2", "Novo Nordisk", "NOVO-B.CO", "Healthcare", 500000000000, "Denmark", "Lars Fruergaard Jorgensen"),
        ("corp_DSV", "DSV A/S", "DSV.CO", "Industrials", 45000000000, "Denmark", "Jens Bjorn Andersen"),
        ("corp_ORSTED", "Orsted A/S", "ORSTED.CO", "Utilities", 25000000000, "Denmark", "Mads Nipper"),
        ("corp_COLOPLAST", "Coloplast A/S", "COLO-B.CO", "Healthcare", 30000000000, "Denmark", "Kristian Villumsen"),
        ("corp_SAMPO", "Sampo Oyj", "SAMPO.HE", "Financial", 25000000000, "Finland", "Torbjorn Magnusson"),
        ("corp_NESTE2", "Neste Oyj", "NESTE.HE", "Energy", 20000000000, "Finland", "Matti Lehmus"),
        ("corp_KONE", "KONE Oyj", "KNEBV.HE", "Industrials", 30000000000, "Finland", "Henrik Ehrnrooth"),
        ("corp_UPM", "UPM-Kymmene", "UPM.HE", "Materials", 20000000000, "Finland", "Jussi Pesonen"),
        ("corp_ENI", "Eni SpA", "ENI.MI", "Energy", 45000000000, "Italy", "Claudio Descalzi"),
        ("corp_STMICRO", "STMicroelectronics", "STM.MI", "Semiconductors", 25000000000, "Italy", "Jean-Marc Chery"),
        ("corp_MONCLER", "Moncler SpA", "MONC.MI", "Consumer Discretionary", 15000000000, "Italy", "Remo Ruffini"),
        ("corp_RACE2", "Ferrari NV", "RACE.MI", "Automotive", 80000000000, "Italy", "Benedetto Vigna"),
        ("corp_GALP", "Galp Energia", "GALP.LS", "Energy", 15000000000, "Portugal", "Filipe Silva"),
        ("corp_EDP", "EDP SA", "EDP.LS", "Utilities", 20000000000, "Portugal", "Miguel Stilwell de Andrade"),
        # ── Asian companies (continued) ──
        ("corp_005930", "Samsung Electronics", "005930.KS", "Technology", 400000000000, "South Korea", "Jay Y. Lee"),
        ("corp_000660", "SK Hynix", "000660.KS", "Semiconductors", 100000000000, "South Korea", "Kwak Noh-Jung"),
        ("corp_035420", "Naver Corp", "035420.KS", "Technology", 30000000000, "South Korea", "Choi Soo-yeon"),
        ("corp_035720", "Kakao Corp", "035720.KS", "Technology", 15000000000, "South Korea", "Hong Eun-taek"),
        ("corp_006400", "Samsung SDI", "006400.KS", "Technology", 20000000000, "South Korea", "Yoonho Choi"),
        ("corp_051910", "LG Chem", "051910.KS", "Materials", 25000000000, "South Korea", "Shin Hak-cheol"),
        ("corp_003670", "POSCO Holdings", "003670.KS", "Materials", 20000000000, "South Korea", "Jang In-hwa"),
        ("corp_373220", "LG Energy Solution", "373220.KS", "Technology", 60000000000, "South Korea", "Kwon Young-soo"),
        ("corp_2330", "TSMC", "2330.TW", "Semiconductors", 900000000000, "Taiwan", "C.C. Wei"),
        ("corp_2317", "Hon Hai (Foxconn)", "2317.TW", "Technology", 50000000000, "Taiwan", "Young Liu"),
        ("corp_2454", "MediaTek Inc", "2454.TW", "Semiconductors", 55000000000, "Taiwan", "Rick Tsai"),
        ("corp_2412", "Chunghwa Telecom", "2412.TW", "Communications", 30000000000, "Taiwan", "Shui-Yi Kuo"),
        ("corp_2308", "Delta Electronics", "2308.TW", "Technology", 30000000000, "Taiwan", "Ping Cheng"),
        ("corp_HDB2", "HDFC Bank", "HDB", "Financial", 140000000000, "India", "Sashidhar Jagdishan"),
        ("corp_WIPRO", "Wipro Ltd", "WIT", "Technology", 30000000000, "India", "Srinivas Pallia"),
        ("corp_HCLTECH", "HCL Technologies", "HCLTECH.NS", "Technology", 50000000000, "India", "C Vijayakumar"),
        ("corp_BAJFINANCE", "Bajaj Finance", "BAJFINANCE.NS", "Financial", 50000000000, "India", "Rajeev Jain"),
        ("corp_MARUTI", "Maruti Suzuki", "MARUTI.NS", "Automotive", 40000000000, "India", "Hisashi Takeuchi"),
        ("corp_LT", "Larsen & Toubro", "LT.NS", "Industrials", 55000000000, "India", "S N Subrahmanyan"),
        ("corp_TITAN", "Titan Company", "TITAN.NS", "Consumer Discretionary", 35000000000, "India", "C K Venkataraman"),
        ("corp_SUNPHARMA", "Sun Pharma", "SUNPHARMA.NS", "Healthcare", 45000000000, "India", "Dilip Shanghvi"),
        ("corp_ADANIENT", "Adani Enterprises", "ADANIENT.NS", "Industrials", 40000000000, "India", "Gautam Adani"),
        ("corp_ADANIPORTS", "Adani Ports", "ADANIPORTS.NS", "Industrials", 35000000000, "India", "Karan Adani"),
        ("corp_ADANIGREEN", "Adani Green Energy", "ADANIGREEN.NS", "Utilities", 25000000000, "India", "Vneet Jaain"),
        ("corp_3690", "Meituan", "3690.HK", "Technology", 100000000000, "China", "Wang Xing"),
        ("corp_1810", "Xiaomi Corp", "1810.HK", "Technology", 70000000000, "China", "Lei Jun"),
        ("corp_9988", "Alibaba Group", "9988.HK", "Technology", 200000000000, "China", "Eddie Wu"),
        ("corp_0700", "Tencent Holdings", "0700.HK", "Technology", 450000000000, "China", "Ma Huateng"),
        ("corp_9618", "JD.com", "9618.HK", "Technology", 50000000000, "China", "Xu Ran"),
        ("corp_1024", "Kuaishou Technology", "1024.HK", "Technology", 20000000000, "China", "Cheng Yixiao"),
        ("corp_0941", "China Mobile", "0941.HK", "Communications", 200000000000, "China", "Yang Jie"),
        ("corp_600519", "Kweichow Moutai", "600519.SS", "Consumer Staples", 250000000000, "China", "Ding Xiongjun"),
        ("corp_601318", "Ping An Insurance", "601318.SS", "Financial", 100000000000, "China", "Ma Mingzhe"),
        ("corp_601857", "PetroChina", "601857.SS", "Energy", 200000000000, "China", "Hou Qijun"),
        ("corp_600036", "China Merchants Bank", "600036.SS", "Financial", 100000000000, "China", "Wang Liang"),
        ("corp_300750", "CATL", "300750.SZ", "Technology", 150000000000, "China", "Zeng Yuqun"),
        ("corp_002594", "BYD Company", "002594.SZ", "Automotive", 100000000000, "China", "Wang Chuanfu"),
        ("corp_7267", "Honda Motor", "7267.T", "Automotive", 55000000000, "Japan", "Toshihiro Mibe"),
        ("corp_6501", "Hitachi Ltd", "6501.T", "Technology", 80000000000, "Japan", "Keiji Kojima"),
        ("corp_6594", "Nidec Corp", "6594.T", "Industrials", 25000000000, "Japan", "Shigenobu Nagamori"),
        ("corp_6367", "Daikin Industries", "6367.T", "Industrials", 55000000000, "Japan", "Masanori Togawa"),
        ("corp_8001", "ITOCHU Corp", "8001.T", "Industrials", 50000000000, "Japan", "Keita Ishii"),
        ("corp_8058", "Mitsubishi Corp", "8058.T", "Industrials", 55000000000, "Japan", "Katsuya Nakanishi"),
        ("corp_8031", "Mitsui & Co", "8031.T", "Industrials", 45000000000, "Japan", "Kenichi Hori"),
        ("corp_9433", "KDDI Corp", "9433.T", "Communications", 70000000000, "Japan", "Makoto Takahashi"),
        ("corp_9432", "NTT Corp", "9432.T", "Communications", 100000000000, "Japan", "Akira Shimada"),
        ("corp_4502", "Takeda Pharma", "4502.T", "Healthcare", 50000000000, "Japan", "Christophe Weber"),
        ("corp_4519", "Chugai Pharmaceutical", "4519.T", "Healthcare", 45000000000, "Japan", "Osamu Nagayama"),
        ("corp_6981", "Murata Manufacturing", "6981.T", "Technology", 40000000000, "Japan", "Norio Nakajima"),
        ("corp_8035", "Tokyo Electron", "8035.T", "Semiconductors", 80000000000, "Japan", "Toshiki Kawai"),
        ("corp_6857", "Advantest Corp", "6857.T", "Semiconductors", 30000000000, "Japan", "Douglas Lefever"),
        ("corp_6146", "Disco Corp", "6146.T", "Semiconductors", 20000000000, "Japan", "Kazuma Sekiya"),
        ("corp_7974", "Nintendo Co", "7974.T", "Technology", 70000000000, "Japan", "Shuntaro Furukawa"),
        # ── Additional S&P 500 remaining companies ──
        ("corp_MOH", "Molina Healthcare", "MOH", "Healthcare", 25000000000, "US", "Joe Zubretsky"),
        ("corp_COR", "Cencora Inc", "COR", "Healthcare", 45000000000, "US", "Steve Collis"),
        ("corp_LH", "Labcorp Holdings", "LH", "Healthcare", 20000000000, "US", "Adam Schechter"),
        ("corp_DGX", "Quest Diagnostics", "DGX", "Healthcare", 18000000000, "US", "Steve Rusckowski"),
        ("corp_CRL", "Charles River Labs", "CRL", "Healthcare", 10000000000, "US", "Jim Foster"),
        ("corp_CTLT", "Catalent Inc", "CTLT", "Healthcare", 12000000000, "US", "Alessandro Maselli"),
        ("corp_TEVA", "Teva Pharmaceutical", "TEVA", "Healthcare", 20000000000, "Israel", "Richard Francis"),
        ("corp_ILMN", "Illumina Inc", "ILMN", "Healthcare", 20000000000, "US", "Jacob Thaysen"),
        ("corp_TECH2", "Bio-Rad Labs", "BIO", "Healthcare", 10000000000, "US", "Norman Schwartz"),
        ("corp_COO", "CooperCompanies", "COO", "Healthcare", 20000000000, "US", "Al White"),
        ("corp_MTD", "Mettler-Toledo", "MTD", "Healthcare", 30000000000, "US", "Patrick Kaltenbach"),
        ("corp_WAT", "Waters Corp", "WAT", "Healthcare", 20000000000, "US", "Udit Batra"),
        ("corp_PKG", "Packaging Corp", "PKG", "Materials", 17000000000, "US", "Mark Kowlzan"),
        ("corp_IP", "International Paper", "IP", "Materials", 20000000000, "US", "Andy Silvernail"),
        ("corp_WRK", "WestRock Co", "WRK", "Materials", 10000000000, "US", "David Sewell"),
        ("corp_AVY", "Avery Dennison", "AVY", "Materials", 18000000000, "US", "Deon Stander"),
        ("corp_SEE", "Sealed Air Corp", "SEE", "Materials", 5000000000, "US", "Ted Doheny"),
        ("corp_CE", "Celanese Corp", "CE", "Materials", 12000000000, "US", "Lori Ryerkerk"),
        ("corp_EMN", "Eastman Chemical", "EMN", "Materials", 12000000000, "US", "Mark Costa"),
        ("corp_PPG", "PPG Industries", "PPG", "Materials", 30000000000, "US", "Tim Knavish"),
        ("corp_VMC", "Vulcan Materials", "VMC", "Materials", 35000000000, "US", "Tom Hill"),
        ("corp_MLM", "Martin Marietta", "MLM", "Materials", 35000000000, "US", "Howard Nye"),
        ("corp_ALB", "Albemarle Corp", "ALB", "Materials", 10000000000, "US", "Kent Masters"),
        ("corp_BALL", "Ball Corp", "BALL", "Materials", 20000000000, "US", "Dan Fisher"),
        ("corp_AMCR", "Amcor plc", "AMCR", "Materials", 15000000000, "Switzerland", "Ron Delia"),
        ("corp_CF", "CF Industries", "CF", "Materials", 15000000000, "US", "Tony Will"),
        ("corp_MOS", "Mosaic Co", "MOS", "Materials", 8000000000, "US", "Bruce Bodine"),
        ("corp_DAL", "Delta Air Lines", "DAL", "Industrials", 35000000000, "US", "Ed Bastian"),
        ("corp_UAL", "United Airlines", "UAL", "Industrials", 25000000000, "US", "Scott Kirby"),
        ("corp_AAL2", "American Airlines", "AAL", "Industrials", 10000000000, "US", "Robert Isom"),
        ("corp_LUV", "Southwest Airlines", "LUV", "Industrials", 18000000000, "US", "Bob Jordan"),
        ("corp_JBHT", "J.B. Hunt Transport", "JBHT", "Industrials", 20000000000, "US", "Shelley Simpson"),
        ("corp_DAY", "Dayforce Inc", "DAY", "Technology", 12000000000, "US", "David Ossip"),
        ("corp_VRNT", "Verint Systems", "VRNT", "Technology", 3000000000, "US", "Dan Bodner"),
        ("corp_MANH", "Manhattan Associates", "MANH", "Technology", 17000000000, "US", "Eddie Capel"),
        ("corp_GWRE", "Guidewire Software", "GWRE", "Technology", 17000000000, "US", "Mike Rosenbaum"),
        ("corp_BSY", "Bentley Systems", "BSY", "Technology", 15000000000, "US", "Greg Bentley"),
        ("corp_PTC", "PTC Inc", "PTC", "Technology", 20000000000, "US", "Jim Heppelmann"),
        ("corp_GLOB", "Globant SA", "GLOB", "Technology", 8000000000, "Argentina", "Martin Migoya"),
        ("corp_GRAB2", "Grab Holdings", "GRAB", "Technology", 15000000000, "Singapore", "Anthony Tan"),
        ("corp_IOT", "Samsara Inc", "IOT", "Technology", 25000000000, "US", "Sanjit Biswas"),
        ("corp_MNDY", "monday.com", "MNDY", "Technology", 12000000000, "Israel", "Roy Mann"),
        ("corp_CELH", "Celsius Holdings", "CELH", "Consumer Staples", 8000000000, "US", "John Fieldly"),
        ("corp_DKNG", "DraftKings", "DKNG", "Consumer Discretionary", 20000000000, "US", "Jason Robins"),
        ("corp_PENN", "Penn Entertainment", "PENN", "Consumer Discretionary", 4000000000, "US", "Jay Snowden"),
        ("corp_LYFT", "Lyft Inc", "LYFT", "Technology", 5000000000, "US", "David Risher"),
        ("corp_CPNG", "Coupang Inc", "CPNG", "Technology", 40000000000, "South Korea", "Bom Kim"),
        ("corp_GTLB", "GitLab Inc", "GTLB", "Technology", 8000000000, "US", "Sid Sijbrandij"),
        ("corp_SMAR", "Smartsheet Inc", "SMAR", "Technology", 8000000000, "US", "Mark Mader"),
        ("corp_DT", "Dynatrace Inc", "DT", "Technology", 15000000000, "US", "Rick McConnell"),
        ("corp_CYBR", "CyberArk Software", "CYBR", "Technology", 12000000000, "Israel", "Matt Cohen"),
        ("corp_TENB", "Tenable Holdings", "TENB", "Technology", 5000000000, "US", "Amit Yoran"),
        ("corp_RPD", "Rapid7 Inc", "RPD", "Technology", 3000000000, "US", "Corey Thomas"),
        ("corp_VRNS", "Varonis Systems", "VRNS", "Technology", 6000000000, "US", "Yaki Faitelson"),
        ("corp_QLYS", "Qualys Inc", "QLYS", "Technology", 5000000000, "US", "Sumedh Thakar"),
        ("corp_CIEN", "Ciena Corp", "CIEN", "Technology", 8000000000, "US", "Gary Smith"),
        ("corp_LITE", "Lumentum Holdings", "LITE", "Technology", 5000000000, "US", "Alan Lowe"),
        ("corp_VIAV", "Viavi Solutions", "VIAV", "Technology", 2500000000, "US", "Oleg Khaykin"),
        ("corp_PSTG", "Pure Storage", "PSTG", "Technology", 18000000000, "US", "Charlie Giancarlo"),
        ("corp_CDAY", "Ceridian HCM", "CDAY", "Technology", 10000000000, "US", "David Ossip"),
        ("corp_EXPE", "Expedia Group", "EXPE", "Technology", 20000000000, "US", "Peter Kern"),
        ("corp_ABNB2", "Airbnb Inc", "ABNB", "Technology", 90000000000, "US", "Brian Chesky"),
        ("corp_NTES", "NetEase Inc", "NTES", "Technology", 55000000000, "China", "William Ding"),
        ("corp_WB", "Weibo Corp", "WB", "Technology", 3000000000, "China", "Gaofei Wang"),
        ("corp_TME", "Tencent Music", "TME", "Technology", 15000000000, "China", "Cussion Pang"),
        ("corp_BILI", "Bilibili Inc", "BILI", "Technology", 8000000000, "China", "Rui Chen"),
        ("corp_ZTO", "ZTO Express", "ZTO", "Industrials", 15000000000, "China", "Meisong Lai"),
        ("corp_YUMC", "Yum China Holdings", "YUMC", "Consumer Discretionary", 20000000000, "China", "Joey Wat"),
        ("corp_MNSO", "MINISO Group", "MNSO", "Retail", 5000000000, "China", "Guofu Ye"),
        ("corp_VNET", "VNET Group", "VNET", "Technology", 2000000000, "China", "Josh Chen"),
        ("corp_KC", "Kingsoft Cloud", "KC", "Technology", 1500000000, "China", "Yulin Zou"),
        ("corp_IQ", "iQIYI Inc", "IQ", "Technology", 3000000000, "China", "Yu Gong"),
        # ── More global companies ──
        ("corp_MIDEA", "Midea Group", "000333.SZ", "Consumer Discretionary", 60000000000, "China", "Fang Hongbo"),
        ("corp_LON_600", "Longi Green Energy", "601012.SS", "Technology", 20000000000, "China", "Li Zhenguo"),
        ("corp_WULIANGYE", "Wuliangye Yibin", "000858.SZ", "Consumer Staples", 80000000000, "China", "Zeng Congqin"),
        ("corp_SHENZHOU", "Shenzhou Intl", "2313.HK", "Consumer Discretionary", 15000000000, "China", "Ma Jianrong"),
        ("corp_AIA", "AIA Group", "1299.HK", "Financial", 80000000000, "Hong Kong", "Lee Yuan Siong"),
        ("corp_2388", "BOC Hong Kong", "2388.HK", "Financial", 30000000000, "Hong Kong", "Liu Jin"),
        ("corp_0016", "SHK Properties", "0016.HK", "Real Estate", 20000000000, "Hong Kong", "Raymond Kwok"),
        ("corp_0012", "Henderson Land", "0012.HK", "Real Estate", 15000000000, "Hong Kong", "Peter Lee Ka-kit"),
        ("corp_1928", "Sands China", "1928.HK", "Consumer Discretionary", 15000000000, "Hong Kong", "Rob Goldstein"),
        ("corp_SBGS", "Sberbank", "SBER.MM", "Financial", 60000000000, "Russia", "Herman Gref"),
        ("corp_GAZP", "Gazprom", "GAZP.MM", "Energy", 50000000000, "Russia", "Alexei Miller"),
        ("corp_ROSN", "Rosneft", "ROSN.MM", "Energy", 70000000000, "Russia", "Igor Sechin"),
        ("corp_LKOH", "Lukoil", "LKOH.MM", "Energy", 45000000000, "Russia", "Vagit Alekperov"),
        ("corp_GMKN", "Nornickel", "GMKN.MM", "Materials", 30000000000, "Russia", "Vladimir Potanin"),
        ("corp_NLMK", "NLMK Group", "NLMK.MM", "Materials", 15000000000, "Russia", "Vladimir Lisin"),
        ("corp_YNDX", "Yandex NV", "YNDX.ME", "Technology", 15000000000, "Russia", "Arkady Volozh"),
        ("corp_QNB2", "QNB Group", "QNBK.QA", "Financial", 45000000000, "Qatar", "Abdulla Al-Khalifa"),
        ("corp_SABIC", "SABIC", "2010.SR", "Materials", 70000000000, "Saudi Arabia", "Abdulrahman Al-Fageeh"),
        ("corp_SNB", "Saudi National Bank", "1180.SR", "Financial", 60000000000, "Saudi Arabia", "Talal Al-Khereiji"),
        ("corp_MAADEN", "Saudi Arabian Mining", "1211.SR", "Materials", 15000000000, "Saudi Arabia", "Robert Wilt"),
        ("corp_EMAAR", "Emaar Properties", "EMAAR.DU", "Real Estate", 20000000000, "UAE", "Amit Jain"),
        ("corp_ETISALAT", "e& (Etisalat)", "ETISALAT.AD", "Communications", 45000000000, "UAE", "Hatem Dowidar"),
        ("corp_IHC", "International Holding Co", "IHC.AD", "Industrials", 200000000000, "UAE", "Syed Basar Shueb"),
        ("corp_GTBK", "Guaranty Trust", "GUARANTY.LG", "Financial", 3000000000, "Nigeria", "Segun Agbaje"),
        ("corp_SAFCOM", "Safaricom plc", "SCOM.NR", "Communications", 10000000000, "Kenya", "Peter Ndegwa"),
        ("corp_SSNC", "Standard Bank Group", "SBK.JO", "Financial", 25000000000, "South Africa", "Sim Tshabalala"),
        ("corp_AGL", "Anglo Gold Ashanti", "ANG.JO", "Materials", 15000000000, "South Africa", "Alberto Calderon"),
        ("corp_SOL", "Sasol Ltd", "SOL.JO", "Energy", 8000000000, "South Africa", "Fleetwood Grobler"),
        ("corp_ITSA", "Itausa SA", "ITSA4.SA", "Financial", 25000000000, "Brazil", "Alfredo Setubal"),
        ("corp_ABEV", "Ambev SA", "ABEV3.SA", "Consumer Staples", 35000000000, "Brazil", "Jean Jereissati"),
        ("corp_RENT", "Localiza Rent a Car", "RENT3.SA", "Industrials", 15000000000, "Brazil", "Bruno Lasansky"),
        ("corp_B3SA", "B3 SA", "B3SA3.SA", "Financial", 15000000000, "Brazil", "Gilson Finkelsztain"),
        ("corp_WEG", "WEG SA", "WEGE3.SA", "Industrials", 30000000000, "Brazil", "Harry Schmelzer Jr"),
        ("corp_FEMSA", "FEMSA", "FEMSAUBD.MX", "Consumer Staples", 30000000000, "Mexico", "Jose Antonio Fernandez"),
        ("corp_CEMEX", "Cemex SAB", "CX", "Materials", 10000000000, "Mexico", "Fernando Gonzalez"),
        ("corp_BIMBO", "Grupo Bimbo", "BIMBOA.MX", "Consumer Staples", 15000000000, "Mexico", "Daniel Servitje"),
        ("corp_BANORTE", "GF Banorte", "GFNORTEO.MX", "Financial", 20000000000, "Mexico", "Carlos Hank Gonzalez"),
        ("corp_SQM", "SQM SA", "SQM", "Materials", 12000000000, "Chile", "Ricardo Ramos"),
        ("corp_COPEC", "Empresas Copec", "COPEC.SN", "Energy", 10000000000, "Chile", "Roberto Angelini"),
        ("corp_ECOPETROL", "Ecopetrol SA", "EC", "Energy", 20000000000, "Colombia", "Ricardo Roa"),
        ("corp_BANCOLOMBIA", "Bancolombia SA", "CIB", "Financial", 10000000000, "Colombia", "Juan Carlos Mora"),
        ("corp_SQ_AU", "Afterpay/Block AU", "SQ2.AX", "Financial", 5000000000, "Australia", "Alyssa Henry"),
        ("corp_WBC", "Westpac Banking", "WBC.AX", "Financial", 80000000000, "Australia", "Peter King"),
        ("corp_ANZ", "ANZ Group", "ANZ.AX", "Financial", 65000000000, "Australia", "Shayne Elliott"),
        ("corp_NAB", "National Australia Bank", "NAB.AX", "Financial", 70000000000, "Australia", "Andrew Irvine"),
        ("corp_MQG", "Macquarie Group", "MQG.AX", "Financial", 50000000000, "Australia", "Shemara Wikramanayake"),
        ("corp_WOW", "Woolworths Group", "WOW.AX", "Retail", 30000000000, "Australia", "Brad Banducci"),
        ("corp_COL2", "Coles Group", "COL.AX", "Retail", 20000000000, "Australia", "Leah Weckert"),
        ("corp_TCL", "Transurban Group", "TCL.AX", "Industrials", 30000000000, "Australia", "Michelle Jablko"),
        ("corp_GMG", "Goodman Group", "GMG.AX", "Real Estate", 50000000000, "Australia", "Greg Goodman"),
        ("corp_ALL_AU", "Aristocrat Leisure", "ALL.AX", "Technology", 30000000000, "Australia", "Trevor Croker"),
        ("corp_XRO", "Xero Ltd", "XRO.AX", "Technology", 15000000000, "New Zealand", "Sukhinder Singh Cassidy"),
        ("corp_FPH", "Fisher & Paykel", "FPH.NZ", "Healthcare", 15000000000, "New Zealand", "Lewis Gradon"),
        ("corp_MFC", "Manulife Financial", "MFC", "Financial", 55000000000, "Canada", "Roy Gori"),
        ("corp_SLF", "Sun Life Financial", "SLF", "Financial", 35000000000, "Canada", "Kevin Strain"),
        ("corp_GWO", "Great-West Lifeco", "GWO.TO", "Financial", 40000000000, "Canada", "Paul Mahon"),
        ("corp_SHOP2", "Shopify Inc", "SHOP.TO", "Technology", 130000000000, "Canada", "Tobi Lutke"),
        ("corp_CSU", "Constellation Software", "CSU.TO", "Technology", 70000000000, "Canada", "Mark Leonard"),
        ("corp_BAM", "Brookfield Asset Mgmt", "BAM", "Financial", 80000000000, "Canada", "Bruce Flatt"),
        ("corp_BN", "Brookfield Corp", "BN", "Financial", 90000000000, "Canada", "Bruce Flatt"),
        ("corp_TRP", "TC Energy Corp", "TRP", "Energy", 45000000000, "Canada", "Francois Poirier"),
        ("corp_SU2", "Suncor Energy", "SU", "Energy", 55000000000, "Canada", "Rich Kruger"),
        ("corp_CNQ", "Canadian Natural Resources", "CNQ", "Energy", 70000000000, "Canada", "Scott Stauth"),
        ("corp_ABX", "Barrick Gold", "GOLD", "Materials", 35000000000, "Canada", "Mark Bristow"),
        ("corp_AEM", "Agnico Eagle Mines", "AEM", "Materials", 40000000000, "Canada", "Ammar Al-Joundi"),
        ("corp_OTEX", "Open Text Corp", "OTEX", "Technology", 10000000000, "Canada", "Mark Barrenechea"),
        ("corp_LSPD", "Lightspeed Commerce", "LSPD", "Technology", 3000000000, "Canada", "Dax Dasilva"),
        # ── Private companies of significance ──
        ("corp_koch_industries", "Koch Industries", "PRIVATE", "Industrials", 115000000000, "US", "Charles Koch"),
        ("corp_cargill", "Cargill Inc", "PRIVATE", "Consumer Staples", 160000000000, "US", "Brian Sikes"),
        ("corp_mars_inc", "Mars Inc", "PRIVATE", "Consumer Staples", 50000000000, "US", "Poul Weihrauch"),
        ("corp_bloomberg_lp", "Bloomberg LP", "PRIVATE", "Financial", 50000000000, "US", "Michael Bloomberg"),
        ("corp_fidelity", "Fidelity Investments", "PRIVATE", "Financial", 4900000000000, "US", "Abigail Johnson"),
        ("corp_chanel", "Chanel SA", "PRIVATE", "Consumer Discretionary", 15000000000, "France", "Leena Nair"),
        ("corp_ikea", "IKEA (Ingka Group)", "PRIVATE", "Retail", 50000000000, "Netherlands", "Jesper Brodin"),
        ("corp_huawei", "Huawei Technologies", "PRIVATE", "Technology", 100000000000, "China", "Ren Zhengfei"),
        ("corp_bytedance", "ByteDance", "PRIVATE", "Technology", 220000000000, "China", "Liang Rubo"),
        ("corp_spacex", "SpaceX", "PRIVATE", "Industrials", 200000000000, "US", "Elon Musk"),
        ("corp_openai", "OpenAI", "PRIVATE", "Technology", 157000000000, "US", "Sam Altman"),
        ("corp_stripe", "Stripe Inc", "PRIVATE", "Financial", 65000000000, "US", "Patrick Collison"),
        ("corp_databricks", "Databricks", "PRIVATE", "Technology", 43000000000, "US", "Ali Ghodsi"),
        ("corp_canva", "Canva", "PRIVATE", "Technology", 26000000000, "Australia", "Melanie Perkins"),
        ("corp_epic_games", "Epic Games", "PRIVATE", "Technology", 30000000000, "US", "Tim Sweeney"),
        ("corp_valve", "Valve Corp", "PRIVATE", "Technology", 10000000000, "US", "Gabe Newell"),
        ("corp_anduril", "Anduril Industries", "PRIVATE", "Industrials", 14000000000, "US", "Palmer Luckey"),
        ("corp_shein", "Shein", "PRIVATE", "Retail", 66000000000, "China", "Xu Yangtian"),
        ("corp_revolut", "Revolut", "PRIVATE", "Financial", 45000000000, "UK", "Nik Storonsky"),
        ("corp_klarna", "Klarna", "PRIVATE", "Financial", 14000000000, "Sweden", "Sebastian Siemiatkowski"),
        ("corp_tiktok_us", "TikTok (US)", "PRIVATE", "Technology", 50000000000, "US", "Shou Chew"),
        ("corp_flexport", "Flexport", "PRIVATE", "Industrials", 8000000000, "US", "Ryan Petersen"),
        ("corp_discord", "Discord Inc", "PRIVATE", "Technology", 15000000000, "US", "Jason Citron"),
        ("corp_fanatics", "Fanatics Inc", "PRIVATE", "Technology", 31000000000, "US", "Michael Rubin"),
        ("corp_plaid", "Plaid Inc", "PRIVATE", "Financial", 13000000000, "US", "Zach Perret"),
        ("corp_figma", "Figma Inc", "PRIVATE", "Technology", 12000000000, "US", "Dylan Field"),
        ("corp_notion", "Notion Labs", "PRIVATE", "Technology", 10000000000, "US", "Ivan Zhao"),
        ("corp_reddit", "Reddit Inc", "RDDT", "Technology", 15000000000, "US", "Steve Huffman"),
        ("corp_kraken", "Kraken Exchange", "PRIVATE", "Financial", 10000000000, "US", "Jesse Powell"),
        ("corp_ripple", "Ripple Labs", "PRIVATE", "Financial", 11000000000, "US", "Brad Garlinghouse"),
        # ── Additional S&P 500 / Russell 1000 companies to reach 1000+ ──
        ("corp_OTIS", "Otis Worldwide", "OTIS", "Industrials", 40000000000, "US", "Judy Marks"),
        ("corp_ALLE", "Allegion plc", "ALLE", "Industrials", 12000000000, "Ireland", "John Stone"),
        ("corp_NDSN", "Nordson Corp", "NDSN", "Industrials", 15000000000, "US", "Sundaram Nagarajan"),
        ("corp_AOS", "A.O. Smith Corp", "AOS", "Industrials", 12000000000, "US", "Kevin Wheeler"),
        ("corp_RRX", "Regal Rexnord", "RRX", "Industrials", 8000000000, "US", "Louis Pinkham"),
        ("corp_TRMK", "Trustmark Corp", "TRMK", "Financial", 2000000000, "US", "Duane Dewey"),
        ("corp_MASI", "Masimo Corp", "MASI", "Healthcare", 8000000000, "US", "Joe Kiani"),
        ("corp_SWAV", "Shockwave Medical", "SWAV", "Healthcare", 10000000000, "US", "Doug Godshall"),
        ("corp_RVMD", "Revolution Medicines", "RVMD", "Healthcare", 8000000000, "US", "Mark Goldsmith"),
        ("corp_SRPT", "Sarepta Therapeutics", "SRPT", "Healthcare", 12000000000, "US", "Doug Ingram"),
        ("corp_HALO", "Halozyme Therapeutics", "HALO", "Healthcare", 12000000000, "US", "Nicole LaBrosse"),
        ("corp_RARE", "Ultragenyx Pharmaceutical", "RARE", "Healthcare", 5000000000, "US", "Emil Kakkis"),
        ("corp_IONS", "Ionis Pharmaceuticals", "IONS", "Healthcare", 8000000000, "US", "Brett Monia"),
        ("corp_ACAD", "Acadia Healthcare", "ACHC", "Healthcare", 5000000000, "US", "Chris Hunter"),
        ("corp_THC", "Tenet Healthcare", "THC", "Healthcare", 12000000000, "US", "Saum Sutaria"),
        ("corp_DVA", "DaVita Inc", "DVA", "Healthcare", 12000000000, "US", "Javier Rodriguez"),
        ("corp_USFD", "US Foods Holding", "USFD", "Consumer Staples", 15000000000, "US", "Dave Flitman"),
        ("corp_PFGC", "Performance Food Group", "PFGC", "Consumer Staples", 10000000000, "US", "George Holm"),
        ("corp_CASY", "Casey's General Stores", "CASY", "Retail", 15000000000, "US", "Darren Rebelez"),
        ("corp_FIVE", "Five Below Inc", "FIVE", "Retail", 8000000000, "US", "Joel Anderson"),
        ("corp_ULTA", "Ulta Beauty", "ULTA", "Retail", 20000000000, "US", "Dave Kimbell"),
        ("corp_WSM", "Williams-Sonoma", "WSM", "Retail", 20000000000, "US", "Laura Alber"),
        ("corp_RH", "RH (Restoration Hardware)", "RH", "Retail", 8000000000, "US", "Gary Friedman"),
        ("corp_POOL", "Pool Corp", "POOL", "Retail", 15000000000, "US", "Peter Arvan"),
        ("corp_TSCO2", "Tractor Supply", "TSCO", "Retail", 30000000000, "US", "Hal Lawton"),
        ("corp_EFX", "Equifax Inc", "EFX", "Financial", 35000000000, "US", "Mark Begor"),
        ("corp_TRU", "TransUnion", "TRU", "Financial", 17000000000, "US", "Chris Cartwright"),
        ("corp_FLT", "Fleetcor Technologies", "FLT", "Technology", 22000000000, "US", "Ron Clarke"),
        ("corp_WU", "Western Union", "WU", "Financial", 4000000000, "US", "Devin McGranahan"),
        ("corp_VICI", "VICI Properties", "VICI", "Real Estate", 35000000000, "US", "Edward Pitoniak"),
        ("corp_INVH", "Invitation Homes", "INVH", "Real Estate", 22000000000, "US", "Dallas Tanner"),
        ("corp_AVB", "AvalonBay Communities", "AVB", "Real Estate", 30000000000, "US", "Ben Schall"),
        ("corp_EQR", "Equity Residential", "EQR", "Real Estate", 28000000000, "US", "Mark Parrell"),
        ("corp_MAA", "Mid-America Apartment", "MAA", "Real Estate", 18000000000, "US", "Eric Bolton"),
        ("corp_UDR", "UDR Inc", "UDR", "Real Estate", 15000000000, "US", "Tom Toomey"),
        ("corp_DLR", "Digital Realty Trust", "DLR", "Real Estate", 45000000000, "US", "Andy Power"),
        ("corp_ARE", "Alexandria Real Estate", "ARE", "Real Estate", 20000000000, "US", "Joel Marcus"),
        ("corp_SBAC", "SBA Communications", "SBAC", "Real Estate", 25000000000, "US", "Brendan Cavanagh"),
        ("corp_ESS", "Essex Property Trust", "ESS", "Real Estate", 18000000000, "US", "Angela Kleiman"),
        ("corp_HST", "Host Hotels & Resorts", "HST", "Real Estate", 12000000000, "US", "Jim Risoleo"),
        ("corp_KIM", "Kimco Realty", "KIM", "Real Estate", 15000000000, "US", "Conor Flynn"),
        ("corp_REG", "Regency Centers", "REG", "Real Estate", 13000000000, "US", "Lisa Palmer"),
        ("corp_NRG", "NRG Energy", "NRG", "Utilities", 18000000000, "US", "Larry Coben"),
        ("corp_AES", "AES Corp", "AES", "Utilities", 10000000000, "US", "Andres Gluski"),
        ("corp_ES", "Eversource Energy", "ES", "Utilities", 23000000000, "US", "Joe Nolan"),
        ("corp_WEC", "WEC Energy Group", "WEC", "Utilities", 28000000000, "US", "Scott Lauber"),
        ("corp_DTE2", "DTE Energy", "DTE", "Utilities", 23000000000, "US", "Jerry Norcia"),
        ("corp_ED", "Consolidated Edison", "ED", "Utilities", 35000000000, "US", "Tim Cawley"),
        ("corp_CMS", "CMS Energy", "CMS", "Utilities", 20000000000, "US", "Garrick Rochow"),
        ("corp_FE", "FirstEnergy Corp", "FE", "Utilities", 25000000000, "US", "Brian Tierney"),
        ("corp_PNW", "Pinnacle West Capital", "PNW", "Utilities", 10000000000, "US", "Jeff Guldner"),
        ("corp_LNT", "Alliant Energy", "LNT", "Utilities", 15000000000, "US", "John Larsen"),
        ("corp_EVRG", "Evergy Inc", "EVRG", "Utilities", 15000000000, "US", "David Campbell"),
        ("corp_ATO", "Atmos Energy", "ATO", "Utilities", 22000000000, "US", "Kevin Akers"),
        ("corp_NI", "NiSource Inc", "NI", "Utilities", 15000000000, "US", "Lloyd Yates"),
        ("corp_OGE", "OGE Energy", "OGE", "Utilities", 8000000000, "US", "Sean Trauschke"),
        ("corp_PEG", "PSEG Inc", "PEG", "Utilities", 35000000000, "US", "Ralph LaRossa"),
        ("corp_PPL", "PPL Corp", "PPL", "Utilities", 23000000000, "US", "Vince Sorgi"),
        ("corp_AWK", "American Water Works", "AWK", "Utilities", 28000000000, "US", "Susan Hardwick"),
        ("corp_WTRG", "Essential Utilities", "WTRG", "Utilities", 12000000000, "US", "Chris Franklin"),
        ("corp_SWX", "Southwest Gas Holdings", "SWX", "Utilities", 5000000000, "US", "Karen Haller"),
        # ── More global companies ──
        ("corp_SKG", "Smurfit Kappa", "SKG.L", "Materials", 15000000000, "Ireland", "Tony Smurfit"),
        ("corp_CRH", "CRH plc", "CRH", "Materials", 60000000000, "Ireland", "Albert Manifold"),
        ("corp_KYOCERA", "Kyocera Corp", "6971.T", "Technology", 20000000000, "Japan", "Hideo Tanimoto"),
        ("corp_FANUC", "Fanuc Corp", "6954.T", "Industrials", 35000000000, "Japan", "Kenji Yamaguchi"),
        ("corp_SMC", "SMC Corp", "6273.T", "Industrials", 35000000000, "Japan", "Yoshiki Takada"),
        ("corp_RECRUIT", "Recruit Holdings", "6098.T", "Technology", 55000000000, "Japan", "Hisayuki Idekoba"),
        ("corp_TOKIO_M", "Tokio Marine", "8766.T", "Financial", 55000000000, "Japan", "Satoru Komiya"),
        ("corp_SOFTBANK_MOBILE", "SoftBank Corp", "9434.T", "Communications", 60000000000, "Japan", "Junichi Miyakawa"),
        ("corp_SUMITOMO", "Sumitomo Corp", "8053.T", "Industrials", 25000000000, "Japan", "Masayuki Hyodo"),
        ("corp_MARUBENI", "Marubeni Corp", "8002.T", "Industrials", 20000000000, "Japan", "Masumi Kakinoki"),
        ("corp_AJINOMOTO", "Ajinomoto Co", "2802.T", "Consumer Staples", 25000000000, "Japan", "Taro Fujie"),
        ("corp_ASAHI", "Asahi Group", "2502.T", "Consumer Staples", 20000000000, "Japan", "Atsushi Katsuki"),
        ("corp_BRIDGESTONE", "Bridgestone Corp", "5108.T", "Automotive", 25000000000, "Japan", "Shuichi Ishibashi"),
        ("corp_SUBARU", "Subaru Corp", "7270.T", "Automotive", 15000000000, "Japan", "Atsushi Osaki"),
        ("corp_SUZUKI", "Suzuki Motor", "7269.T", "Automotive", 20000000000, "Japan", "Toshihiro Suzuki"),
        ("corp_PANASONIC", "Panasonic Holdings", "6752.T", "Technology", 25000000000, "Japan", "Yuki Kusumi"),
        ("corp_TOSHIBA", "Toshiba Corp", "6502.T", "Technology", 15000000000, "Japan", "Taro Shimada"),
        ("corp_NEC", "NEC Corp", "6701.T", "Technology", 15000000000, "Japan", "Takayuki Morita"),
        ("corp_FUJITSU", "Fujitsu Ltd", "6702.T", "Technology", 25000000000, "Japan", "Takahito Tokita"),
        ("corp_TREND_MICRO", "Trend Micro", "4704.T", "Technology", 10000000000, "Japan", "Eva Chen"),
        ("corp_RENESAS", "Renesas Electronics", "6723.T", "Semiconductors", 25000000000, "Japan", "Hidetoshi Shibata"),
        ("corp_ROHM", "Rohm Co", "6963.T", "Semiconductors", 8000000000, "Japan", "Isao Matsumoto"),
        ("corp_AMORE", "Amorepacific", "090430.KS", "Consumer Staples", 8000000000, "South Korea", "Suh Kyung-bae"),
        ("corp_HYUNDAI_MOTOR", "Hyundai Motor", "005380.KS", "Automotive", 50000000000, "South Korea", "Jaehoon Chang"),
        ("corp_KIA", "Kia Corp", "000270.KS", "Automotive", 30000000000, "South Korea", "Song Ho-sung"),
        ("corp_SAMSUNG_BIO", "Samsung Biologics", "207940.KS", "Healthcare", 40000000000, "South Korea", "John Rim"),
        ("corp_HANWHA", "Hanwha Corp", "000880.KS", "Industrials", 10000000000, "South Korea", "Kim Seung-yeon"),
        ("corp_SK_INN", "SK Innovation", "096770.KS", "Energy", 10000000000, "South Korea", "Park Sang-kyu"),
        ("corp_ACER", "Acer Inc", "2353.TW", "Technology", 5000000000, "Taiwan", "Jason Chen"),
        ("corp_ASUS", "ASUSTeK Computer", "2357.TW", "Technology", 15000000000, "Taiwan", "SY Hsu"),
        ("corp_REALTEK", "Realtek Semiconductor", "2379.TW", "Semiconductors", 12000000000, "Taiwan", "Shou-Kuo Chen"),
        ("corp_NOVATEK_TW", "Novatek Microelectronics", "3034.TW", "Semiconductors", 10000000000, "Taiwan", "Vic Kuo"),
        ("corp_ASE", "ASE Technology", "3711.TW", "Semiconductors", 15000000000, "Taiwan", "Jason Chang"),
        ("corp_UMC", "United Microelectronics", "2303.TW", "Semiconductors", 15000000000, "Taiwan", "SC Chien"),
        ("corp_HUL", "Hindustan Unilever", "HINDUNILVR.NS", "Consumer Staples", 60000000000, "India", "Rohit Jawa"),
        ("corp_NESTLEIND", "Nestle India", "NESTLEIND.NS", "Consumer Staples", 25000000000, "India", "Suresh Narayanan"),
        ("corp_ASIANPAINTS", "Asian Paints", "ASIANPAINT.NS", "Materials", 30000000000, "India", "Amit Syngle"),
        ("corp_DMART", "Avenue Supermarts", "DMART.NS", "Retail", 30000000000, "India", "Neville Noronha"),
        ("corp_PIDILITIND", "Pidilite Industries", "PIDILITIND.NS", "Materials", 15000000000, "India", "Bharat Puri"),
        ("corp_HAVELLS", "Havells India", "HAVELLS.NS", "Industrials", 15000000000, "India", "Anil Rai Gupta"),
        ("corp_GODREJCP", "Godrej Consumer", "GODREJCP.NS", "Consumer Staples", 12000000000, "India", "Sudhir Sitapati"),
        ("corp_ZOMATO", "Zomato Ltd", "ZOMATO.NS", "Technology", 20000000000, "India", "Deepinder Goyal"),
        ("corp_PAYTM", "One 97 Communications", "PAYTM.NS", "Technology", 5000000000, "India", "Vijay Shekhar Sharma"),
        ("corp_POLYCAB", "Polycab India", "POLYCAB.NS", "Industrials", 15000000000, "India", "Inder Jaisinghani"),
        ("corp_TATAPOWER", "Tata Power", "TATAPOWER.NS", "Utilities", 15000000000, "India", "Praveer Sinha"),
        ("corp_JSWSTEEL", "JSW Steel", "JSWSTEEL.NS", "Materials", 20000000000, "India", "Jayant Acharya"),
        ("corp_TATASTEEL", "Tata Steel", "TATASTEEL.NS", "Materials", 15000000000, "India", "TV Narendran"),
        ("corp_HINDALCO", "Hindalco Industries", "HINDALCO.NS", "Materials", 15000000000, "India", "Satish Pai"),
        ("corp_POWERGRID", "Power Grid Corp", "POWERGRID.NS", "Utilities", 20000000000, "India", "Ravinder Kumar Tyagi"),
        ("corp_NTPC", "NTPC Ltd", "NTPC.NS", "Utilities", 25000000000, "India", "Gurdeep Singh"),
        ("corp_COALINDIA", "Coal India", "COALINDIA.NS", "Energy", 20000000000, "India", "PM Prasad"),
        ("corp_ONGC", "Oil & Natural Gas Corp", "ONGC.NS", "Energy", 25000000000, "India", "Arun Kumar Singh"),
        ("corp_IOC", "Indian Oil Corp", "IOC.NS", "Energy", 20000000000, "India", "Shrikant Madhav Vaidya"),
        ("corp_BPCL", "Bharat Petroleum", "BPCL.NS", "Energy", 15000000000, "India", "G Krishnakumar"),
        ("corp_INDIGO", "InterGlobe Aviation", "INDIGO.NS", "Industrials", 15000000000, "India", "Pieter Elbers"),
        ("corp_DRREDDY", "Dr. Reddy's Labs", "DRREDDY.NS", "Healthcare", 12000000000, "India", "Erez Israeli"),
        ("corp_CIPLA", "Cipla Ltd", "CIPLA.NS", "Healthcare", 12000000000, "India", "Umang Vohra"),
        ("corp_LUPIN", "Lupin Ltd", "LUPIN.NS", "Healthcare", 10000000000, "India", "Vinita Gupta"),
        ("corp_BIOCON", "Biocon Ltd", "BIOCON.NS", "Healthcare", 5000000000, "India", "Kiran Mazumdar-Shaw"),
        ("corp_GRASIM", "Grasim Industries", "GRASIM.NS", "Materials", 20000000000, "India", "KK Maheshwari"),
        ("corp_TECHM", "Tech Mahindra", "TECHM.NS", "Technology", 15000000000, "India", "Mohit Joshi"),
        ("corp_LTIM", "LTIMindtree", "LTIM.NS", "Technology", 15000000000, "India", "Debashis Chatterjee"),
        # ── Final batch: more S&P 500 + international to reach 1000+ ──
        ("corp_NDAQ2", "Nasdaq Inc", "NDAQ", "Financial", 40000000000, "US", "Adena Friedman"),
        ("corp_IFF", "International Flavors", "IFF", "Materials", 22000000000, "US", "Erik Fyrwald"),
        ("corp_FMC", "FMC Corp", "FMC", "Materials", 6000000000, "US", "Mark Douglas"),
        ("corp_HII", "Huntington Ingalls", "HII", "Industrials", 12000000000, "US", "Chris Kastner"),
        ("corp_LHX", "L3Harris Technologies", "LHX", "Industrials", 45000000000, "US", "Chris Kubasik"),
        ("corp_BWXT", "BWX Technologies", "BWXT", "Industrials", 12000000000, "US", "Rex Geveden"),
        ("corp_HEI", "HEICO Corp", "HEI", "Industrials", 30000000000, "US", "Laurans Mendelson"),
        ("corp_RBC", "RBC Bearings", "RBC", "Industrials", 8000000000, "US", "Michael Hartnett"),
        ("corp_SPR", "Spirit AeroSystems", "SPR", "Industrials", 5000000000, "US", "Pat Shanahan"),
        ("corp_TXT", "Textron Inc", "TXT", "Industrials", 15000000000, "US", "Scott Donnelly"),
        ("corp_CW", "Curtiss-Wright", "CW", "Industrials", 12000000000, "US", "Lynn Bamford"),
        ("corp_BURL", "Burlington Stores", "BURL", "Retail", 18000000000, "US", "Michael O'Sullivan"),
        ("corp_GPS", "Gap Inc", "GPS", "Retail", 8000000000, "US", "Richard Dickson"),
        ("corp_ANF", "Abercrombie & Fitch", "ANF", "Retail", 8000000000, "US", "Fran Horowitz"),
        ("corp_GRMN", "Garmin Ltd", "GRMN", "Technology", 35000000000, "US", "Cliff Pemble"),
        ("corp_AAON", "AAON Inc", "AAON", "Industrials", 10000000000, "US", "Gary Fields"),
        ("corp_WSC", "WillScot Mobile Mini", "WSC", "Industrials", 8000000000, "US", "Brad Soultz"),
        ("corp_KNSL", "Kinsale Capital", "KNSL", "Financial", 12000000000, "US", "Michael Kehoe"),
        ("corp_RYAN", "Ryan Specialty", "RYAN", "Financial", 15000000000, "US", "Pat Ryan"),
        ("corp_MSTR", "MicroStrategy", "MSTR", "Technology", 50000000000, "US", "Phong Le"),
        ("corp_SSNC2", "SS&C Technologies", "SSNC", "Technology", 15000000000, "US", "Bill Stone"),
        ("corp_TYL", "Tyler Technologies", "TYL", "Technology", 22000000000, "US", "Lynn Moore"),
        ("corp_TNET", "TriNet Group", "TNET", "Technology", 5000000000, "US", "Mike Simonds"),
        ("corp_BJ", "BJ's Wholesale Club", "BJ", "Retail", 12000000000, "US", "Bob Eddy"),
        ("corp_WING", "Wingstop Inc", "WING", "Consumer Discretionary", 10000000000, "US", "Michael Skipworth"),
        ("corp_CMG", "Chipotle Mexican Grill", "CMG", "Consumer Discretionary", 75000000000, "US", "Scott Boatwright"),
        ("corp_DPZ", "Domino's Pizza", "DPZ", "Consumer Discretionary", 17000000000, "US", "Russell Weiner"),
        ("corp_YUM", "Yum! Brands", "YUM", "Consumer Discretionary", 40000000000, "US", "David Gibbs"),
        ("corp_QSR", "Restaurant Brands Intl", "QSR", "Consumer Discretionary", 30000000000, "Canada", "Josh Kobza"),
        ("corp_DINO", "HF Sinclair Corp", "DINO", "Energy", 8000000000, "US", "Tim Go"),
        ("corp_CLR", "Continental Resources", "CLR", "Energy", 20000000000, "US", "Harold Hamm"),
        ("corp_AR", "Antero Resources", "AR", "Energy", 8000000000, "US", "Paul Rady"),
        ("corp_EQT", "EQT Corp", "EQT", "Energy", 20000000000, "US", "Toby Rice"),
        ("corp_RRC", "Range Resources", "RRC", "Energy", 8000000000, "US", "Dennis Degner"),
        ("corp_CTRA", "Coterra Energy", "CTRA", "Energy", 20000000000, "US", "Tom Jorden"),
        ("corp_OVV", "Ovintiv Inc", "OVV", "Energy", 12000000000, "US", "Brendan McCracken"),
        ("corp_VLO2", "Valero Energy", "VLO", "Energy", 50000000000, "US", "Lane Riggs"),
        ("corp_KDP", "Keurig Dr Pepper", "KDP", "Consumer Staples", 50000000000, "US", "Tim Cofer"),
        ("corp_MDLZ2", "Mondelez Intl", "MDLZ", "Consumer Staples", 90000000000, "US", "Dirk Van de Put"),
        ("corp_CPB", "Campbell Soup", "CPB", "Consumer Staples", 12000000000, "US", "Mark Clouse"),
        ("corp_CAG", "Conagra Brands", "CAG", "Consumer Staples", 15000000000, "US", "Sean Connolly"),
        ("corp_MKC", "McCormick & Co", "MKC", "Consumer Staples", 20000000000, "US", "Brendan Foley"),
        ("corp_J2", "Lamb Weston Holdings", "LW", "Consumer Staples", 10000000000, "US", "Mike Smith"),
        ("corp_POST", "Post Holdings", "POST", "Consumer Staples", 7000000000, "US", "Rob Vitale"),
        ("corp_BG", "Bunge Global", "BG", "Consumer Staples", 12000000000, "US", "Greg Heckman"),
        ("corp_ADM", "Archer-Daniels-Midland", "ADM", "Consumer Staples", 25000000000, "US", "Juan Luciano"),
        ("corp_DE2", "Deere & Co", "DE", "Industrials", 135000000000, "US", "John May"),
        ("corp_TECK", "Teck Resources", "TECK", "Materials", 20000000000, "Canada", "Jonathan Price"),
        ("corp_FM", "First Quantum Minerals", "FM.TO", "Materials", 10000000000, "Canada", "Tristan Pascall"),
        ("corp_SHOP3", "Shopify Inc", "SHOP", "Technology", 130000000000, "Canada", "Tobi Lutke"),
        ("corp_GIB", "CGI Inc", "GIB.A.TO", "Technology", 25000000000, "Canada", "George Schindler"),
        ("corp_WSP", "WSP Global", "WSP.TO", "Industrials", 25000000000, "Canada", "Alexandre L'Heureux"),
        ("corp_DOL", "Dollarama Inc", "DOL.TO", "Retail", 25000000000, "Canada", "Neil Rossy"),
        ("corp_ATD", "Alimentation Couche-Tard", "ATD.TO", "Retail", 65000000000, "Canada", "Alex Miller"),
        ("corp_RCI", "Rogers Communications", "RCI-B.TO", "Communications", 25000000000, "Canada", "Tony Staffieri"),
        ("corp_BCE", "BCE Inc", "BCE.TO", "Communications", 30000000000, "Canada", "Mirko Bibic"),
        ("corp_T_CA", "Telus Corp", "T.TO", "Communications", 25000000000, "Canada", "Darren Entwistle"),
        ("corp_MFC2", "Manulife Financial", "MFC.TO", "Financial", 55000000000, "Canada", "Roy Gori"),
        ("corp_POW", "Power Corp of Canada", "POW.TO", "Financial", 20000000000, "Canada", "Jeffrey Orr"),
    ]


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4: CONNECTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def get_connections():
    """
    Returns list of (actor_a, actor_b, relationship, strength, evidence_json).
    Strength: 0.0–1.0
    """
    connections = []

    # ── Helper for building evidence ──
    def ev(source, confidence):
        return json.dumps([{"source": source, "confidence": confidence}])

    # ═══════════════════════════════════════════════
    # CEO → Company (employed_by)
    # ═══════════════════════════════════════════════
    ceo_company = [
        ("bil_cook", "corp_AAPL", 0.95),
        ("bil_nadella", "corp_MSFT", 0.95),
        ("bil_jensen_huang", "corp_NVDA", 0.95),
        ("bil_pichai", "corp_GOOG", 0.95),
        ("bil_jassy", "corp_AMZN", 0.95),
        ("bil_zuckerberg", "corp_META", 0.95),
        ("bil_ellison", "corp_ORCL", 0.95),
        ("bil_dimon", "corp_JPM", 0.95),
        ("bil_musk", "corp_TSLA", 0.95),
        ("bil_son", "corp_9984", 0.95),
        ("bil_ambani", "corp_RELIANCE", 0.90),
        ("bil_jay_y_lee", "corp_samsung", 0.90),
        ("bil_terry_gou", "corp_TSM", 0.30),  # Former Foxconn, related
        ("bil_schwarzman", "corp_BX", 0.95),
        ("bil_marc_rowan", "corp_APO", 0.95),
        ("bil_dorsey", "corp_SQ", 0.90),
        ("bil_chesky", "corp_ABNB", 0.95),
        ("bil_spiegel", "corp_SNAP", 0.95),
        ("bil_hastings", "corp_NFLX", 0.80),
        ("bil_altman", "corp_openai", 0.95),  # Not public but notable
        ("bil_daniel_ek", "corp_SPOT", 0.95),
        ("bil_thiel", "corp_PLTR", 0.80),
        ("bil_brian_armstrong", "corp_COIN", 0.95),
        ("bil_tadashi_yanai", "corp_9983", 0.95),
        ("bil_son", "corp_9984", 0.95),
        ("bil_rupert_murdoch", "corp_fox", 0.80),
        ("bil_dell", "corp_dell_tech", 0.95),
        ("bil_bloomberg", "corp_bloomberg_lp", 0.95),
    ]
    for (a, b, s) in ceo_company:
        connections.append((a, b, "employed_by", s, ev("public_record", "confirmed")))

    # ═══════════════════════════════════════════════
    # Founder → Company (co_founder)
    # ═══════════════════════════════════════════════
    founder_company = [
        ("bil_musk", "corp_TSLA", 0.90),
        ("bil_bezos", "corp_AMZN", 0.95),
        ("bil_zuckerberg", "corp_META", 0.95),
        ("bil_page", "corp_GOOG", 0.95),
        ("bil_brin", "corp_GOOG", 0.95),
        ("bil_gates", "corp_MSFT", 0.95),
        ("bil_ellison", "corp_ORCL", 0.95),
        ("bil_jensen_huang", "corp_NVDA", 0.95),
        ("bil_phil_knight", "corp_NKE", 0.95),
        ("bil_brian_chesky", "corp_ABNB", 0.95),
        ("bil_joe_gebbia", "corp_ABNB", 0.95),
        ("bil_nathan_blecharczyk", "corp_ABNB", 0.95),
        ("bil_evan_spiegel", "corp_SNAP", 0.95),
        ("bil_bobby_murphy", "corp_SNAP", 0.95),
        ("bil_dustin_moskovitz", "corp_META", 0.90),
        ("bil_eduardo_saverin", "corp_META", 0.85),
        ("bil_patrick_collison", "corp_stripe", 0.95),
        ("bil_john_collison", "corp_stripe", 0.95),
        ("bil_mike_cannon_brookes", "corp_TEAM", 0.95),
        ("bil_scott_farquhar", "corp_TEAM", 0.95),
        ("bil_dorsey", "corp_SQ", 0.95),
        ("bil_marc_andreessen", "corp_a16z", 0.95),
        ("bil_dalio", "corp_bridgewater", 0.95),
        ("bil_griffin", "corp_citadel", 0.95),
        ("bil_ma", "corp_BABA", 0.95),
        ("bil_ma_huateng", "corp_TCEHY", 0.95),
        ("bil_arnault", "corp_MC", 0.95),
        ("bil_slim", "corp_AMX", 0.90),
        ("bil_daniel_ek", "corp_SPOT", 0.95),
        ("bil_martin_lorentzon", "corp_SPOT", 0.95),
        ("bil_brian_armstrong", "corp_COIN", 0.95),
        ("bil_palmer_luckey", "corp_anduril", 0.95),
        ("bil_michael_saylor", "corp_microstrategy", 0.95),
        ("bil_changpeng_zhao", "corp_binance", 0.95),
        ("bil_drew_houston", "corp_dropbox", 0.95),
        ("bil_reid_hoffman", "corp_linkedin", 0.95),
    ]
    for (a, b, s) in founder_company:
        connections.append((a, b, "co_founder", s, ev("public_record", "confirmed")))

    # ═══════════════════════════════════════════════
    # Family connections
    # ═══════════════════════════════════════════════
    family = [
        # Walton family
        ("bil_walton_jim", "bil_walton_rob", 0.95),
        ("bil_walton_jim", "bil_walton_alice", 0.95),
        ("bil_walton_rob", "bil_walton_alice", 0.95),
        ("bil_walton_jim", "bil_lukas_walton", 0.90),
        # Koch family
        ("bil_koch_charles", "bil_koch_julia", 0.85),
        # Mars family
        ("bil_john_mars", "bil_jacqueline_mars", 0.95),
        # Wertheimer brothers (Chanel)
        ("bil_wertheimer_alain", "bil_wertheimer_gerard", 0.95),
        # Hartono brothers
        ("bil_hartono_robert", "bil_hartono_michael", 0.95),
        # Lauder family
        ("bil_leonard_lauder", "bil_ronald_lauder", 0.95),
        # Quandt/Klatten (BMW)
        ("bil_stefan_quandt", "bil_susanne_klatten", 0.95),
        # Albrecht (Aldi)
        ("bil_karl_albrecht_jr", "bil_theo_albrecht_jr", 0.90),
        ("bil_karl_albrecht_jr", "bil_beate_heister", 0.90),
        # Ofer brothers
        ("bil_eyal_ofer", "bil_idan_ofer", 0.95),
        # Sawiris brothers
        ("bil_nassef_sawiris", "bil_naguib_sawiris", 0.95),
        # Kamath brothers (Zerodha)
        ("bil_nikhil_kamath", "bil_nithin_kamath", 0.95),
        # Collison brothers (Stripe)
        ("bil_patrick_collison", "bil_john_collison", 0.95),
        # Trump family
        ("bil_donald_trump", "gov_us_trump", 0.99),
        # Murdoch family
        ("bil_rupert_murdoch", "corp_fox", 0.90),
        # 3G Capital partners
        ("bil_jorge_lemann", "bil_marcel_herrmann", 0.90),
        # Apollo co-founders
        ("bil_leon_black", "bil_marc_rowan", 0.85),
        ("bil_leon_black", "bil_josh_harris", 0.85),
        ("bil_marc_rowan", "bil_josh_harris", 0.85),
        # Google co-founders
        ("bil_page", "bil_brin", 0.95),
        # Airbnb co-founders
        ("bil_brian_chesky", "bil_joe_gebbia", 0.90),
        ("bil_brian_chesky", "bil_nathan_blecharczyk", 0.90),
        # Snap co-founders
        ("bil_evan_spiegel", "bil_bobby_murphy", 0.90),
        # Atlassian co-founders
        ("bil_mike_cannon_brookes", "bil_scott_farquhar", 0.90),
        # Samsung / Lee family
        ("bil_jay_y_lee", "corp_samsung", 0.95),
        # Hyundai / Chung family
        ("bil_chung_euisun", "corp_hyundai", 0.90),
    ]
    for (a, b, s) in family:
        connections.append((a, b, "family", s, ev("public_record", "confirmed")))

    # ═══════════════════════════════════════════════
    # Political donors (US — from OpenSecrets / FEC data)
    # ═══════════════════════════════════════════════
    donors = [
        # Republican donors
        ("bil_griffin", "gov_us_trump", "political_donor", 0.80, ev("opensecrets", "public_record")),
        ("bil_thiel", "gov_us_trump", "political_donor", 0.85, ev("opensecrets", "public_record")),
        ("bil_adelson_miriam", "gov_us_trump", "political_donor", 0.90, ev("opensecrets", "public_record")),
        ("bil_koch_charles", "gov_us_trump", "political_donor", 0.60, ev("opensecrets", "public_record")),
        ("bil_schwarzman", "gov_us_trump", "political_donor", 0.75, ev("opensecrets", "public_record")),
        ("bil_singer", "gov_us_trump", "political_donor", 0.70, ev("opensecrets", "public_record")),
        ("bil_robert_mercer", "gov_us_trump", "political_donor", 0.85, ev("opensecrets", "public_record")),
        ("bil_harold_hamm", "gov_us_trump", "political_donor", 0.80, ev("opensecrets", "public_record")),
        ("bil_musk", "gov_us_trump", "political_donor", 0.90, ev("news_reports", "confirmed")),
        ("bil_steve_wynn", "gov_us_trump", "political_donor", 0.75, ev("opensecrets", "public_record")),
        # Democrat donors
        ("bil_soros", "congress_pelosi", "political_donor", 0.60, ev("opensecrets", "public_record")),
        ("bil_bloomberg", "congress_pelosi", "political_donor", 0.65, ev("opensecrets", "public_record")),
        ("bil_dustin_moskovitz", "congress_pelosi", "political_donor", 0.50, ev("opensecrets", "public_record")),
        ("bil_reid_hoffman", "congress_pelosi", "political_donor", 0.55, ev("opensecrets", "public_record")),
        ("bil_simons_jim", "congress_pelosi", "political_donor", 0.60, ev("opensecrets", "public_record")),
        # Major tech lobbying
        ("corp_AAPL", "gov_us_trump", "lobbies_for", 0.60, ev("opensecrets", "public_record")),
        ("corp_GOOG", "gov_us_trump", "lobbies_for", 0.60, ev("opensecrets", "public_record")),
        ("corp_META", "gov_us_trump", "lobbies_for", 0.60, ev("opensecrets", "public_record")),
        ("corp_AMZN", "gov_us_trump", "lobbies_for", 0.60, ev("opensecrets", "public_record")),
        ("corp_MSFT", "gov_us_trump", "lobbies_for", 0.60, ev("opensecrets", "public_record")),
    ]
    connections.extend(donors)

    # ═══════════════════════════════════════════════
    # Central bank appointments (appointed_by)
    # ═══════════════════════════════════════════════
    appointments = [
        ("gov_us_trump", "fed_powell", "appointed_by", 0.95, ev("public_record", "confirmed")),
        ("gov_france_macron", "ecb_lagarde", "appointed_by", 0.70, ev("public_record", "confirmed")),
        ("gov_china_xi", "pboc_pan", "appointed_by", 0.90, ev("public_record", "confirmed")),
        ("gov_japan_ishiba", "boj_ueda", "appointed_by", 0.80, ev("public_record", "confirmed")),
        ("gov_us_trump", "treasury_yellen", "appointed_by", 0.30, ev("public_record", "confirmed")),  # Biden appointed, low strength since Trump in office
    ]
    connections.extend(appointments)

    # ═══════════════════════════════════════════════
    # Diplomatic relationships
    # ═══════════════════════════════════════════════
    diplomatic = [
        # Close alliances
        ("gov_us_trump", "gov_israel_netanyahu", "diplomatic_relationship", 0.90, ev("public_record", "confirmed")),
        ("gov_us_trump", "gov_saudi_mbs", "diplomatic_relationship", 0.85, ev("news_reports", "confirmed")),
        ("gov_us_trump", "gov_uk_starmer", "diplomatic_relationship", 0.70, ev("public_record", "confirmed")),
        ("gov_us_trump", "gov_japan_ishiba", "diplomatic_relationship", 0.75, ev("public_record", "confirmed")),
        ("gov_us_trump", "gov_india_modi", "diplomatic_relationship", 0.80, ev("news_reports", "confirmed")),
        ("gov_us_trump", "gov_north_korea_kim", "diplomatic_relationship", 0.40, ev("news_reports", "estimated")),
        ("gov_us_trump", "gov_hungary_orban", "diplomatic_relationship", 0.75, ev("news_reports", "confirmed")),
        ("gov_us_trump", "gov_argentina_milei", "diplomatic_relationship", 0.80, ev("news_reports", "confirmed")),
        ("gov_us_trump", "gov_el_salvador_bukele", "diplomatic_relationship", 0.70, ev("news_reports", "estimated")),
        # China relationships
        ("gov_china_xi", "gov_russia_putin", "diplomatic_relationship", 0.90, ev("public_record", "confirmed")),
        ("gov_china_xi", "gov_us_trump", "diplomatic_relationship", 0.50, ev("news_reports", "confirmed")),
        ("gov_china_xi", "gov_north_korea_kim", "diplomatic_relationship", 0.80, ev("public_record", "confirmed")),
        ("gov_china_xi", "gov_pakistan_sharif", "diplomatic_relationship", 0.75, ev("public_record", "confirmed")),
        ("gov_china_xi", "gov_iran_khamenei", "diplomatic_relationship", 0.70, ev("news_reports", "confirmed")),
        # Russia relationships
        ("gov_russia_putin", "gov_iran_khamenei", "diplomatic_relationship", 0.80, ev("public_record", "confirmed")),
        ("gov_russia_putin", "gov_north_korea_kim", "diplomatic_relationship", 0.85, ev("news_reports", "confirmed")),
        ("gov_russia_putin", "gov_india_modi", "diplomatic_relationship", 0.65, ev("news_reports", "confirmed")),
        ("gov_russia_putin", "gov_turkey_erdogan", "diplomatic_relationship", 0.60, ev("news_reports", "confirmed")),
        ("gov_russia_putin", "gov_belarus_lukashenko", "diplomatic_relationship", 0.90, ev("public_record", "confirmed")),
        # EU internal
        ("gov_france_macron", "gov_germany_scholz", "diplomatic_relationship", 0.80, ev("public_record", "confirmed")),
        ("gov_france_macron", "gov_uk_starmer", "diplomatic_relationship", 0.65, ev("news_reports", "confirmed")),
        ("gov_france_macron", "gov_italy_meloni", "diplomatic_relationship", 0.55, ev("news_reports", "confirmed")),
        ("gov_germany_scholz", "gov_poland_tusk", "diplomatic_relationship", 0.70, ev("public_record", "confirmed")),
        # Middle East
        ("gov_saudi_mbs", "gov_uae_mbz", "diplomatic_relationship", 0.85, ev("public_record", "confirmed")),
        ("gov_saudi_mbs", "gov_qatar_tamim", "diplomatic_relationship", 0.50, ev("news_reports", "estimated")),
        ("gov_israel_netanyahu", "gov_uae_mbz", "diplomatic_relationship", 0.70, ev("public_record", "confirmed")),
        ("gov_iran_khamenei", "gov_syria_sharaa", "diplomatic_relationship", 0.30, ev("news_reports", "estimated")),
        ("gov_iran_khamenei", "gov_iraq_sudani", "diplomatic_relationship", 0.70, ev("news_reports", "confirmed")),
        # Africa
        ("gov_south_africa_ramaphosa", "gov_china_xi", "diplomatic_relationship", 0.70, ev("public_record", "confirmed")),
        ("gov_ethiopia_ahmed", "gov_uae_mbz", "diplomatic_relationship", 0.65, ev("news_reports", "confirmed")),
        # BRICS alignment
        ("gov_india_modi", "gov_brazil_lula", "diplomatic_relationship", 0.60, ev("public_record", "confirmed")),
        ("gov_brazil_lula", "gov_china_xi", "diplomatic_relationship", 0.65, ev("public_record", "confirmed")),
        # Rivalries / tensions
        ("gov_us_trump", "gov_china_xi", "competitor", 0.80, ev("news_reports", "confirmed")),
        ("gov_us_trump", "gov_russia_putin", "competitor", 0.60, ev("news_reports", "confirmed")),
        ("gov_india_modi", "gov_china_xi", "competitor", 0.65, ev("news_reports", "confirmed")),
        ("gov_israel_netanyahu", "gov_iran_khamenei", "competitor", 0.95, ev("public_record", "confirmed")),
        ("gov_ukraine_zelensky", "gov_russia_putin", "competitor", 0.99, ev("public_record", "confirmed")),
        ("gov_taiwan_lai", "gov_china_xi", "competitor", 0.85, ev("public_record", "confirmed")),
        ("gov_south_korea_yoon", "gov_north_korea_kim", "competitor", 0.90, ev("public_record", "confirmed")),
    ]
    connections.extend(diplomatic)

    # ═══════════════════════════════════════════════
    # Leader → Country same_party / political
    # ═══════════════════════════════════════════════
    political_alliances = [
        ("gov_us_trump", "congress_crenshaw", "same_party", 0.80, ev("public_record", "confirmed")),
        ("gov_saudi_mbs", "royal_mbs", "family", 0.99, ev("public_record", "confirmed")),
        ("gov_uae_mbz", "royal_mbz", "family", 0.99, ev("public_record", "confirmed")),
        ("gov_saudi_salman", "gov_saudi_mbs", "family", 0.99, ev("public_record", "confirmed")),
    ]
    connections.extend(political_alliances)

    # ═══════════════════════════════════════════════
    # Business partnerships / co_investor
    # ═══════════════════════════════════════════════
    business = [
        # Major investment partnerships
        ("bil_buffett", "corp_AAPL", "co_investor", 0.90, ev("sec_filing", "public_record")),
        ("bil_buffett", "corp_KO", "co_investor", 0.95, ev("sec_filing", "public_record")),
        ("bil_buffett", "corp_BAC", "co_investor", 0.90, ev("sec_filing", "public_record")),
        ("bil_buffett", "corp_BRK", "employed_by", 0.99, ev("public_record", "confirmed")),
        ("am_fink", "corp_BLK", "employed_by", 0.99, ev("public_record", "confirmed")),
        ("bil_soros", "corp_GOOG", "co_investor", 0.50, ev("sec_filing", "public_record")),
        ("bil_dalio", "corp_GOOG", "co_investor", 0.40, ev("sec_filing", "public_record")),
        ("bil_griffin", "corp_META", "co_investor", 0.50, ev("sec_filing", "public_record")),
        ("bil_abigail_johnson", "corp_fidelity", "employed_by", 0.99, ev("public_record", "confirmed")),
        # Waltons and Walmart
        ("bil_walton_jim", "corp_WMT", "co_investor", 0.99, ev("sec_filing", "public_record")),
        ("bil_walton_rob", "corp_WMT", "co_investor", 0.99, ev("sec_filing", "public_record")),
        ("bil_walton_alice", "corp_WMT", "co_investor", 0.99, ev("sec_filing", "public_record")),
        ("bil_lukas_walton", "corp_WMT", "co_investor", 0.95, ev("sec_filing", "public_record")),
        # Tech moguls' holdings
        ("bil_bezos", "corp_AMZN", "co_investor", 0.95, ev("sec_filing", "public_record")),
        ("bil_page", "corp_GOOG", "co_investor", 0.95, ev("sec_filing", "public_record")),
        ("bil_brin", "corp_GOOG", "co_investor", 0.95, ev("sec_filing", "public_record")),
        ("bil_gates", "corp_MSFT", "co_investor", 0.80, ev("sec_filing", "public_record")),
        ("bil_ballmer", "corp_MSFT", "co_investor", 0.85, ev("sec_filing", "public_record")),
        ("bil_dell", "corp_dell_tech", "co_investor", 0.95, ev("sec_filing", "public_record")),
        ("bil_eric_schmidt", "corp_GOOG", "co_investor", 0.70, ev("sec_filing", "public_record")),
        # SoftBank Vision Fund
        ("bil_son", "corp_UBER", "co_investor", 0.75, ev("sec_filing", "public_record")),
        ("bil_son", "corp_SE", "co_investor", 0.70, ev("sec_filing", "public_record")),
        ("bil_son", "corp_GRAB", "co_investor", 0.70, ev("sec_filing", "public_record")),
        # PE firms and portfolio
        ("bil_schwarzman", "corp_BX", "employed_by", 0.99, ev("public_record", "confirmed")),
        ("bil_kravis", "corp_KKR", "employed_by", 0.95, ev("public_record", "confirmed")),
        # Supply chain / business relationships
        ("corp_AAPL", "corp_TSM", "business_partner", 0.95, ev("public_record", "confirmed")),
        ("corp_NVDA", "corp_TSM", "business_partner", 0.95, ev("public_record", "confirmed")),
        ("corp_AMD", "corp_TSM", "business_partner", 0.90, ev("public_record", "confirmed")),
        ("corp_QCOM", "corp_TSM", "business_partner", 0.85, ev("public_record", "confirmed")),
        ("corp_AAPL", "corp_samsung", "business_partner", 0.70, ev("news_reports", "confirmed")),
        ("corp_NVDA", "corp_ASML", "business_partner", 0.85, ev("public_record", "confirmed")),
        ("corp_TSM", "corp_ASML", "business_partner", 0.90, ev("public_record", "confirmed")),
        ("corp_samsung", "corp_ASML", "business_partner", 0.85, ev("public_record", "confirmed")),
        # Arnault / LVMH
        ("bil_arnault", "corp_MC", "employed_by", 0.99, ev("public_record", "confirmed")),
        # Pharma partnerships
        ("corp_LLY", "corp_NVO", "competitor", 0.80, ev("news_reports", "confirmed")),
        ("corp_PFE", "corp_MRNA", "competitor", 0.75, ev("news_reports", "confirmed")),
        # 3G Capital / AB InBev
        ("bil_jorge_lemann", "corp_BUD", "co_investor", 0.85, ev("sec_filing", "public_record")),
        ("bil_marcel_herrmann", "corp_BUD", "co_investor", 0.80, ev("sec_filing", "public_record")),
        # Indian business connections
        ("bil_ambani", "corp_RELIANCE", "employed_by", 0.99, ev("public_record", "confirmed")),
        ("bil_adani", "gov_india_modi", "business_partner", 0.70, ev("news_reports", "estimated")),
        ("bil_ambani", "gov_india_modi", "business_partner", 0.65, ev("news_reports", "estimated")),
        # Saudi investments
        ("gov_saudi_mbs", "corp_ARAMCO", "regulatory_relationship", 0.95, ev("public_record", "confirmed")),
        ("bil_alwaleed", "corp_AAPL", "co_investor", 0.60, ev("sec_filing", "public_record")),
        ("bil_alwaleed", "corp_twitter", "co_investor", 0.50, ev("sec_filing", "public_record")),
    ]
    connections.extend(business)

    # ═══════════════════════════════════════════════
    # Competitive relationships
    # ═══════════════════════════════════════════════
    competitors = [
        # Mega tech
        ("corp_AAPL", "corp_samsung", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_AAPL", "corp_GOOG", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_MSFT", "corp_GOOG", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_MSFT", "corp_AMZN", "competitor", 0.80, ev("industry", "confirmed")),  # Cloud
        ("corp_AMZN", "corp_WMT", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_AMZN", "corp_GOOG", "competitor", 0.70, ev("industry", "confirmed")),  # Cloud
        ("corp_META", "corp_GOOG", "competitor", 0.80, ev("industry", "confirmed")),  # Ads
        ("corp_META", "corp_SNAP", "competitor", 0.75, ev("industry", "confirmed")),
        ("corp_META", "corp_TCEHY", "competitor", 0.60, ev("industry", "confirmed")),
        # Semiconductors
        ("corp_NVDA", "corp_AMD", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_NVDA", "corp_INTC", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_AMD", "corp_INTC", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_TSM", "corp_samsung", "competitor", 0.85, ev("industry", "confirmed")),  # Foundry
        ("corp_AVGO", "corp_QCOM", "competitor", 0.70, ev("industry", "confirmed")),
        # Streaming
        ("corp_NFLX", "corp_DIS", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_NFLX", "corp_AMZN", "competitor", 0.75, ev("industry", "confirmed")),
        ("corp_NFLX", "corp_AAPL", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_SPOT", "corp_AAPL", "competitor", 0.80, ev("industry", "confirmed")),
        # Banking
        ("corp_JPM", "corp_GS", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_JPM", "corp_MS", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_JPM", "corp_BAC", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_GS", "corp_MS", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_BLK", "corp_BX", "competitor", 0.75, ev("industry", "confirmed")),
        ("corp_BLK", "corp_KKR", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_BX", "corp_KKR", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_BX", "corp_APO", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_KKR", "corp_APO", "competitor", 0.80, ev("industry", "confirmed")),
        # Payments
        ("corp_V", "corp_MA", "competitor", 0.95, ev("industry", "confirmed")),
        ("corp_V", "corp_PYPL", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_SQ", "corp_PYPL", "competitor", 0.80, ev("industry", "confirmed")),
        # Cloud/SaaS
        ("corp_CRM", "corp_MSFT", "competitor", 0.75, ev("industry", "confirmed")),
        ("corp_NOW", "corp_CRM", "competitor", 0.65, ev("industry", "confirmed")),
        ("corp_SNOW", "corp_DDOG", "competitor", 0.60, ev("industry", "confirmed")),
        # Cybersecurity
        ("corp_CRWD", "corp_PANW", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_CRWD", "corp_ZS", "competitor", 0.70, ev("industry", "confirmed")),
        # EDA
        ("corp_SNPS", "corp_CDNS", "competitor", 0.90, ev("industry", "confirmed")),
        # Ride-sharing
        ("corp_UBER", "corp_DASH", "competitor", 0.70, ev("industry", "confirmed")),
        # E-commerce China
        ("corp_BABA", "corp_JD", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_BABA", "corp_PDD", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_JD", "corp_PDD", "competitor", 0.80, ev("industry", "confirmed")),
        # EV
        ("corp_TSLA", "corp_BYD", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_TSLA", "corp_RIVN", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_TSLA", "corp_LCID", "competitor", 0.60, ev("industry", "confirmed")),
        ("corp_TSLA", "corp_NIO", "competitor", 0.65, ev("industry", "confirmed")),
        ("corp_BYD", "corp_NIO", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_BYD", "corp_XPEV", "competitor", 0.65, ev("industry", "confirmed")),
        ("corp_BYD", "corp_LI", "competitor", 0.65, ev("industry", "confirmed")),
        # Auto traditional
        ("corp_TM", "corp_GM", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_TM", "corp_F", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_TM", "corp_VOW", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_GM", "corp_F", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_BMW", "corp_MBG", "competitor", 0.90, ev("industry", "confirmed")),
        # Pharma
        ("corp_JNJ", "corp_PFE", "competitor", 0.75, ev("industry", "confirmed")),
        ("corp_ABBV", "corp_AMGN", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_MRK", "corp_BMY", "competitor", 0.75, ev("industry", "confirmed")),
        ("corp_ROG", "corp_NOVN", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_AZN", "corp_SAN_FR", "competitor", 0.75, ev("industry", "confirmed")),
        # Energy
        ("corp_XOM", "corp_CVX", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_XOM", "corp_SHEL", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_XOM", "corp_BP", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_SHEL", "corp_BP", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_TTE", "corp_SHEL", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_ARAMCO", "corp_XOM", "competitor", 0.75, ev("industry", "confirmed")),
        # Defense
        ("corp_LMT", "corp_RTX", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_LMT", "corp_NOC", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_LMT", "corp_BA", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_LMT", "corp_GD", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_BA", "corp_STLA", "competitor", 0.40, ev("industry", "confirmed")),  # Aerospace
        # Retail
        ("corp_COST", "corp_WMT", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_HD", "corp_LOW", "competitor", 0.95, ev("industry", "confirmed")),
        ("corp_TJX", "corp_ROST", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_ITX", "corp_NKE", "competitor", 0.50, ev("industry", "confirmed")),
        # Luxury
        ("corp_MC", "corp_RMS", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_MC", "corp_OR", "competitor", 0.60, ev("industry", "confirmed")),
        # Telecom
        ("corp_T", "corp_VZ", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_T", "corp_CMCSA", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_DTE", "corp_ERIC", "competitor", 0.40, ev("industry", "confirmed")),
        # Mining
        ("corp_BHP", "corp_RIO", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_BHP", "corp_VALE", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_FCX", "corp_NEM", "competitor", 0.70, ev("industry", "confirmed")),
        # Food & Beverage
        ("corp_KO", "corp_PEP", "competitor", 0.95, ev("industry", "confirmed")),
        ("corp_MCD", "corp_SBUX", "competitor", 0.60, ev("industry", "confirmed")),
        ("corp_NESN", "corp_UL", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_MDLZ", "corp_HSY", "competitor", 0.70, ev("industry", "confirmed")),
    ]
    connections.extend(competitors)

    # ═══════════════════════════════════════════════
    # Board member connections (major cross-board)
    # ═══════════════════════════════════════════════
    board = [
        ("bil_gates", "corp_BRK", "board_member", 0.80, ev("sec_filing", "public_record")),  # Former
        ("bil_eric_schmidt", "corp_AAPL", "board_member", 0.50, ev("sec_filing", "public_record")),  # Former
        ("bil_john_doerr", "corp_GOOG", "board_member", 0.85, ev("sec_filing", "public_record")),
        ("bil_thiel", "corp_META", "board_member", 0.60, ev("sec_filing", "public_record")),  # Former
        ("bil_marc_andreessen", "corp_META", "board_member", 0.85, ev("sec_filing", "public_record")),
        ("bil_ackman", "corp_BRK", "co_investor", 0.50, ev("sec_filing", "public_record")),
    ]
    connections.extend(board)

    # ═══════════════════════════════════════════════
    # Regulatory relationships
    # ═══════════════════════════════════════════════
    regulatory = [
        ("fed_powell", "corp_JPM", "regulatory_relationship", 0.80, ev("public_record", "confirmed")),
        ("fed_powell", "corp_BAC", "regulatory_relationship", 0.80, ev("public_record", "confirmed")),
        ("fed_powell", "corp_GS", "regulatory_relationship", 0.80, ev("public_record", "confirmed")),
        ("fed_powell", "corp_MS", "regulatory_relationship", 0.80, ev("public_record", "confirmed")),
        ("fed_powell", "corp_C", "regulatory_relationship", 0.80, ev("public_record", "confirmed")),
        ("fed_powell", "corp_WFC", "regulatory_relationship", 0.80, ev("public_record", "confirmed")),
        ("ecb_lagarde", "corp_BNP", "regulatory_relationship", 0.75, ev("public_record", "confirmed")),
        ("ecb_lagarde", "corp_ALV", "regulatory_relationship", 0.70, ev("public_record", "confirmed")),
        ("boj_ueda", "corp_8306", "regulatory_relationship", 0.75, ev("public_record", "confirmed")),
        ("gov_china_xi", "corp_BABA", "regulatory_relationship", 0.90, ev("news_reports", "confirmed")),
        ("gov_china_xi", "corp_TCEHY", "regulatory_relationship", 0.85, ev("news_reports", "confirmed")),
        ("gov_china_xi", "corp_PDD", "regulatory_relationship", 0.80, ev("news_reports", "confirmed")),
    ]
    connections.extend(regulatory)

    # ═══════════════════════════════════════════════
    # Acquisition relationships
    # ═══════════════════════════════════════════════
    acquisitions = [
        ("corp_MSFT", "corp_activision", "acquisition", 0.95, ev("sec_filing", "confirmed")),
        ("corp_AMZN", "corp_mgm", "acquisition", 0.95, ev("sec_filing", "confirmed")),
        ("corp_AVGO", "corp_vmware", "acquisition", 0.95, ev("sec_filing", "confirmed")),
        ("corp_ORCL", "corp_cerner", "acquisition", 0.95, ev("sec_filing", "confirmed")),
    ]
    connections.extend(acquisitions)

    # ═══════════════════════════════════════════════
    # Billionaire-to-billionaire relationships
    # ═══════════════════════════════════════════════
    bil_to_bil = [
        # Known partnerships
        ("bil_musk", "bil_thiel", "business_partner", 0.80, ev("public_record", "confirmed")),  # PayPal Mafia
        ("bil_thiel", "bil_reid_hoffman", "business_partner", 0.75, ev("public_record", "confirmed")),
        ("bil_thiel", "bil_palmer_luckey", "co_investor", 0.80, ev("news_reports", "confirmed")),
        ("bil_gates", "bil_buffett", "business_partner", 0.90, ev("public_record", "confirmed")),  # Giving Pledge
        ("bil_buffett", "bil_dimon", "business_partner", 0.60, ev("news_reports", "confirmed")),
        ("bil_soros", "bil_dalio", "competitor", 0.50, ev("inference", "inferred")),
        ("bil_griffin", "bil_cohen_steve", "competitor", 0.70, ev("industry", "confirmed")),
        ("bil_schwarzman", "bil_kravis", "competitor", 0.65, ev("industry", "confirmed")),
        ("bil_schwarzman", "bil_marc_rowan", "competitor", 0.60, ev("industry", "confirmed")),
        ("bil_marc_andreessen", "bil_thiel", "co_investor", 0.70, ev("news_reports", "confirmed")),
        ("bil_vinod_khosla", "bil_john_doerr", "co_investor", 0.65, ev("news_reports", "confirmed")),
        # Bezos/Musk rivalry
        ("bil_musk", "bil_bezos", "competitor", 0.90, ev("news_reports", "confirmed")),
        # Indian billionaire connections
        ("bil_ambani", "bil_adani", "competitor", 0.80, ev("news_reports", "confirmed")),
        ("bil_shiv_nadar", "bil_azim_premji", "competitor", 0.70, ev("industry", "confirmed")),
        # China tech
        ("bil_ma", "bil_ma_huateng", "competitor", 0.80, ev("industry", "confirmed")),
        # Russia oligarchs
        ("bil_vladimir_potanin", "bil_alisher_usmanov", "business_partner", 0.50, ev("news_reports", "estimated")),
        ("bil_roman_abramovich", "gov_russia_putin", "business_partner", 0.70, ev("news_reports", "estimated")),
        ("bil_vladimir_potanin", "gov_russia_putin", "business_partner", 0.65, ev("news_reports", "estimated")),
        ("bil_alisher_usmanov", "gov_russia_putin", "business_partner", 0.60, ev("news_reports", "estimated")),
        ("bil_gennady_timchenko", "gov_russia_putin", "business_partner", 0.85, ev("news_reports", "confirmed")),
        # Korean chaebols
        ("bil_jay_y_lee", "bil_chung_euisun", "business_partner", 0.40, ev("inference", "inferred")),
        # Bloomberg politics
        ("bil_bloomberg", "gov_us_trump", "competitor", 0.60, ev("news_reports", "confirmed")),
    ]
    connections.extend(bil_to_bil)

    # ═══════════════════════════════════════════════
    # Leader → International org
    # ═══════════════════════════════════════════════
    intl = [
        ("gov_us_trump", "gov_nato_rutte", "diplomatic_relationship", 0.70, ev("public_record", "confirmed")),
        ("gov_us_trump", "gov_un_guterres", "diplomatic_relationship", 0.40, ev("news_reports", "confirmed")),
        ("gov_france_macron", "gov_eu_vonderleyen", "diplomatic_relationship", 0.80, ev("public_record", "confirmed")),
        ("gov_germany_scholz", "gov_eu_vonderleyen", "diplomatic_relationship", 0.80, ev("public_record", "confirmed")),
        ("gov_india_modi", "gov_imf_georgieva", "diplomatic_relationship", 0.60, ev("public_record", "confirmed")),
        ("gov_china_xi", "gov_wb_banga", "diplomatic_relationship", 0.50, ev("public_record", "confirmed")),
    ]
    connections.extend(intl)

    # ═══════════════════════════════════════════════
    # Major shareholders → company (confirmed from SEC/proxy)
    # ═══════════════════════════════════════════════
    shareholders = [
        ("bil_musk", "corp_TSLA", "co_investor", 0.95, ev("sec_filing", "public_record")),
        ("bil_zuckerberg", "corp_META", "co_investor", 0.95, ev("sec_filing", "public_record")),
        ("bil_bezos", "corp_AMZN", "co_investor", 0.90, ev("sec_filing", "public_record")),
        ("bil_ellison", "corp_ORCL", "co_investor", 0.90, ev("sec_filing", "public_record")),
        ("bil_phil_knight", "corp_NKE", "co_investor", 0.90, ev("sec_filing", "public_record")),
        ("bil_arnault", "corp_MC", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_bettencourt", "corp_OR", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_hermes_family", "corp_RMS", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_stefan_persson", "corp_hm", "co_investor", 0.90, ev("public_record", "confirmed")),
        ("bil_amancio_ortega", "corp_ITX", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_giovanni_ferrero", "corp_ferrero", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_dieter_schwarz", "corp_lidl", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_gina_rinehart", "corp_hancock", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_andrew_forrest", "corp_FMG", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_thomas_peterffy", "corp_ibkr", "co_investor", 0.95, ev("sec_filing", "public_record")),
        ("bil_robert_kraft", "corp_kraft_group", "employed_by", 0.95, ev("public_record", "confirmed")),
        ("bil_jerry_jones", "corp_dallas_cowboys", "employed_by", 0.95, ev("public_record", "confirmed")),
        ("bil_david_thomson", "corp_thomson_reuters", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_charles_schwab", "corp_SCHW", "co_investor", 0.80, ev("sec_filing", "public_record")),
        ("bil_changpeng_zhao", "corp_binance", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_brian_armstrong", "corp_COIN", "co_investor", 0.90, ev("sec_filing", "public_record")),
        # BlackRock holds positions in essentially every major company
        ("am_fink", "corp_BLK", "co_investor", 0.95, ev("sec_filing", "public_record")),
    ]
    connections.extend(shareholders)

    # ═══════════════════════════════════════════════
    # Additional CEO → Company connections (bulk)
    # ═══════════════════════════════════════════════
    more_ceo_company = [
        # US Tech CEOs
        ("bil_marc_benioff", "corp_CRM", 0.95),
        ("bil_jay_chaudhry", "corp_ZS", 0.95),
        ("bil_gabe_newell", "corp_valve", 0.95),
        ("bil_tim_sweeney", "corp_epic_games", 0.95),
        ("bil_howard_schultz", "corp_SBUX", 0.70),  # Former
        ("bil_pierre_omidyar", "corp_EBAY", 0.50),  # Founder
        ("bil_frank_slootman", "corp_SNOW", 0.70),  # Former CEO
        ("bil_scott_cook", "corp_INTU", 0.60),  # Founder
        # International CEOs
        ("bil_akio_toyoda", "corp_TM", 0.80),
        ("bil_shigenobu_nagamori", "corp_6594", 0.90),
        ("bil_ambani", "corp_RELIANCE", 0.99),
        ("bil_chung_euisun", "corp_hyundai", 0.95),
        ("bil_koo_kwang_mo", "corp_lg", 0.90),
        ("bil_terry_gou", "corp_2317", 0.80),
        ("bil_morris_chang", "corp_TSM", 0.60),  # Founder, retired
        ("bil_ivan_glasenberg", "corp_GLEN", 0.70),  # Former CEO
        ("bil_koos_bekker", "corp_NASPERS", 0.70),  # Former CEO
        ("bil_juan_roig", "corp_mercadona", 0.95),
        ("bil_florentino_perez", "corp_acs", 0.90),
        ("bil_del_vecchio_estate", "corp_essilux", 0.90),
    ]
    for (a, b, s) in more_ceo_company:
        connections.append((a, b, "employed_by", s, ev("public_record", "confirmed")))

    # ═══════════════════════════════════════════════
    # Additional competitor relationships (S&P 500 pairs)
    # ═══════════════════════════════════════════════
    more_competitors = [
        # Hospitality
        ("corp_MAR", "corp_HLT", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_MAR", "corp_H", "competitor", 0.75, ev("industry", "confirmed")),
        ("corp_HLT", "corp_H", "competitor", 0.75, ev("industry", "confirmed")),
        # Cruise lines
        ("corp_RCL", "corp_CCL", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_RCL", "corp_NCLH", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_CCL", "corp_NCLH", "competitor", 0.80, ev("industry", "confirmed")),
        # Casinos
        ("corp_LVS", "corp_MGM", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_LVS", "corp_WYNN", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_MGM", "corp_WYNN", "competitor", 0.85, ev("industry", "confirmed")),
        # Semis EDA equipment
        ("corp_AMAT", "corp_LRCX", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_AMAT", "corp_KLAC", "competitor", 0.75, ev("industry", "confirmed")),
        ("corp_LRCX", "corp_KLAC", "competitor", 0.75, ev("industry", "confirmed")),
        ("corp_8035", "corp_AMAT", "competitor", 0.80, ev("industry", "confirmed")),
        # Memory
        ("corp_MU", "corp_000660", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_MU", "corp_samsung", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_000660", "corp_samsung", "competitor", 0.80, ev("industry", "confirmed")),
        # Storage
        ("corp_WDC", "corp_STX", "competitor", 0.90, ev("industry", "confirmed")),
        # PCs/Servers
        ("corp_DELL", "corp_HPQ", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_DELL", "corp_HPE", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_HPQ", "corp_HPE", "competitor", 0.60, ev("industry", "confirmed")),
        # Networking
        ("corp_CSCO", "corp_JNPR", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_CSCO", "corp_ANET", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_ANET", "corp_JNPR", "competitor", 0.75, ev("industry", "confirmed")),
        # IT services
        ("corp_ACN", "corp_CTSH", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_ACN", "corp_INFY", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_ACN", "corp_TCS", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_CTSH", "corp_INFY", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_TCS", "corp_INFY", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_TCS", "corp_WIPRO", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_INFY", "corp_WIPRO", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_HCLTECH", "corp_WIPRO", "competitor", 0.80, ev("industry", "confirmed")),
        # Enterprise SaaS
        ("corp_CRM", "corp_NOW", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_WDAY", "corp_ADP", "competitor", 0.65, ev("industry", "confirmed")),
        ("corp_VEEV", "corp_CRM", "competitor", 0.60, ev("industry", "confirmed")),
        ("corp_HUBS", "corp_CRM", "competitor", 0.65, ev("industry", "confirmed")),
        ("corp_SNOW", "corp_DDOG", "competitor", 0.65, ev("industry", "confirmed")),
        ("corp_SNOW", "corp_MDB", "competitor", 0.60, ev("industry", "confirmed")),
        ("corp_ESTC", "corp_DDOG", "competitor", 0.60, ev("industry", "confirmed")),
        # Data/Analytics
        ("corp_PLTR", "corp_SNOW", "competitor", 0.55, ev("industry", "confirmed")),
        ("corp_IT", "corp_MSCI", "competitor", 0.50, ev("industry", "confirmed")),
        # Fintech
        ("corp_COIN", "corp_HOOD", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_SOFI", "corp_HOOD", "competitor", 0.65, ev("industry", "confirmed")),
        ("corp_AFRM", "corp_SQ", "competitor", 0.60, ev("industry", "confirmed")),
        ("corp_AFRM", "corp_PYPL", "competitor", 0.55, ev("industry", "confirmed")),
        ("corp_ADYEN", "corp_PYPL", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_ADYEN", "corp_SQ", "competitor", 0.65, ev("industry", "confirmed")),
        # Insurance
        ("corp_MET", "corp_PRU", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_ALL", "corp_TRV", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_CB", "corp_AIG", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_AFL", "corp_MET", "competitor", 0.65, ev("industry", "confirmed")),
        ("corp_AON", "corp_MMC", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_AON", "corp_WTW", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_MMC", "corp_WTW", "competitor", 0.85, ev("industry", "confirmed")),
        # Regional banking
        ("corp_PNC", "corp_USB", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_PNC", "corp_TFC", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_USB", "corp_TFC", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_COF", "corp_DFS", "competitor", 0.85, ev("industry", "confirmed")),
        # Canadian banks
        ("corp_RY", "corp_TD", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_RY", "corp_BMO", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_TD", "corp_BNS", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_BMO", "corp_BNS", "competitor", 0.80, ev("industry", "confirmed")),
        # European banks
        ("corp_HSBC", "corp_BARC", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_HSBC", "corp_BNP", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_BNP", "corp_GLE", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_BNP", "corp_ACA", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_SAN", "corp_BBVA", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_ISP", "corp_UCG", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_UBS", "corp_GS", "competitor", 0.75, ev("industry", "confirmed")),
        # Healthcare distributors
        ("corp_MCK", "corp_CAH", "competitor", 0.90, ev("industry", "confirmed")),
        # Medical devices
        ("corp_MDT", "corp_BSX", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_MDT", "corp_SYK", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_BSX", "corp_SYK", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_BSX", "corp_EW", "competitor", 0.75, ev("industry", "confirmed")),
        ("corp_BDX", "corp_BAX", "competitor", 0.75, ev("industry", "confirmed")),
        # Diagnostics
        ("corp_TMO", "corp_A", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_TMO", "corp_DHR", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_A", "corp_DHR", "competitor", 0.80, ev("industry", "confirmed")),
        # Aerospace/Defense
        ("corp_BA", "corp_EAF", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_GD", "corp_NOC", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_RTX", "corp_NOC", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_BA_L", "corp_LMT", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_RR", "corp_GE", "competitor", 0.75, ev("industry", "confirmed")),
        # Logistics
        ("corp_FDX", "corp_UPS", "competitor", 0.95, ev("industry", "confirmed")),
        ("corp_FDX", "corp_MAERSK", "competitor", 0.40, ev("industry", "confirmed")),
        ("corp_UPS", "corp_MAERSK", "competitor", 0.35, ev("industry", "confirmed")),
        # Railways
        ("corp_UNP", "corp_CSX", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_UNP", "corp_NSC", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_CSX", "corp_NSC", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_CNR", "corp_CP", "competitor", 0.85, ev("industry", "confirmed")),
        # Waste management
        ("corp_WM", "corp_RSG", "competitor", 0.90, ev("industry", "confirmed")),
        # Utilities
        ("corp_NEE", "corp_DUK", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_NEE", "corp_SO", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_DUK", "corp_SO", "competitor", 0.75, ev("industry", "confirmed")),
        ("corp_D", "corp_AEP", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_CEG", "corp_VST", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_SRE", "corp_EXC", "competitor", 0.65, ev("industry", "confirmed")),
        # REITs
        ("corp_PLD", "corp_PSA", "competitor", 0.50, ev("industry", "confirmed")),
        ("corp_AMT", "corp_CCI", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_EQIX", "corp_AMT", "competitor", 0.60, ev("industry", "confirmed")),
        ("corp_SPG", "corp_O", "competitor", 0.60, ev("industry", "confirmed")),
        # Exchanges
        ("corp_CME", "corp_ICE", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_CME", "corp_CBOE", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_ICE", "corp_NDAQ", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_NDAQ", "corp_CBOE", "competitor", 0.75, ev("industry", "confirmed")),
        ("corp_LSEG", "corp_DB1", "competitor", 0.80, ev("industry", "confirmed")),
        # Credit ratings
        ("corp_SPGI", "corp_MCO", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_SPGI", "corp_MSCI", "competitor", 0.60, ev("industry", "confirmed")),
        # Consumer staples Europe
        ("corp_NESN", "corp_MDLZ", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_UL", "corp_PG", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_RECKITT", "corp_PG", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_HEN3", "corp_PG", "competitor", 0.60, ev("industry", "confirmed")),
        # Luxury
        ("corp_MC", "corp_RMS", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_BURBERRY", "corp_MC", "competitor", 0.55, ev("industry", "confirmed")),
        ("corp_MONCLER", "corp_MC", "competitor", 0.50, ev("industry", "confirmed")),
        # Sportswear
        ("corp_NKE", "corp_ADIDAS", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_NKE", "corp_PUMA", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_NKE", "corp_LULU", "competitor", 0.65, ev("industry", "confirmed")),
        ("corp_ADIDAS", "corp_PUMA", "competitor", 0.80, ev("industry", "confirmed")),
        # Japanese auto
        ("corp_TM", "corp_HMC", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_TM", "corp_7267", "competitor", 0.80, ev("industry", "confirmed")),
        # Korean vs Japanese
        ("corp_samsung", "corp_6758", "competitor", 0.75, ev("industry", "confirmed")),
        # Indian banks
        ("corp_HDFCBANK", "corp_ICICIBANK", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_HDFCBANK", "corp_SBIN", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_ICICIBANK", "corp_SBIN", "competitor", 0.80, ev("industry", "confirmed")),
        # Chinese banks
        ("corp_ICBC", "corp_CCB", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_ICBC", "corp_CMB", "competitor", 0.80, ev("industry", "confirmed")),
        # Chinese tech
        ("corp_TCEHY", "corp_BABA", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_BABA", "corp_MEITUAN", "competitor", 0.65, ev("industry", "confirmed")),
        ("corp_1810", "corp_samsung", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_BYD", "corp_TSLA", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_BYD", "corp_TM", "competitor", 0.70, ev("industry", "confirmed")),
        # Telco
        ("corp_T", "corp_VZ", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_DTE", "corp_VOD", "competitor", 0.60, ev("industry", "confirmed")),
        ("corp_TEF", "corp_ORA", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_BHARTI", "corp_0941", "competitor", 0.40, ev("industry", "confirmed")),
        # Oil majors
        ("corp_ARAMCO", "corp_PBR", "competitor", 0.60, ev("industry", "confirmed")),
        ("corp_EQUINOR", "corp_TTE", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_EQUINOR", "corp_SHEL", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_ENI", "corp_TTE", "competitor", 0.75, ev("industry", "confirmed")),
        ("corp_REP", "corp_ENI", "competitor", 0.70, ev("industry", "confirmed")),
        # Latin American
        ("corp_MELI", "corp_BABA", "competitor", 0.40, ev("industry", "confirmed")),
        ("corp_MELI", "corp_AMZN", "competitor", 0.50, ev("industry", "confirmed")),
        ("corp_NU", "corp_ITUB", "competitor", 0.70, ev("industry", "confirmed")),
        # SE Asian tech
        ("corp_SE", "corp_GRAB", "competitor", 0.80, ev("industry", "confirmed")),
    ]
    connections.extend(more_competitors)

    # ═══════════════════════════════════════════════
    # Additional business partnerships & supply chain
    # ═══════════════════════════════════════════════
    more_business = [
        # Apple supply chain
        ("corp_AAPL", "corp_AVGO", "business_partner", 0.85, ev("public_record", "confirmed")),
        ("corp_AAPL", "corp_QCOM", "business_partner", 0.80, ev("public_record", "confirmed")),
        ("corp_AAPL", "corp_2317", "business_partner", 0.90, ev("public_record", "confirmed")),  # Foxconn
        ("corp_AAPL", "corp_MU", "business_partner", 0.70, ev("public_record", "confirmed")),
        ("corp_AAPL", "corp_LRCX", "business_partner", 0.50, ev("inference", "inferred")),
        # NVIDIA supply chain
        ("corp_NVDA", "corp_SMCI", "business_partner", 0.85, ev("public_record", "confirmed")),
        ("corp_NVDA", "corp_DELL", "business_partner", 0.80, ev("public_record", "confirmed")),
        ("corp_NVDA", "corp_HPE", "business_partner", 0.75, ev("public_record", "confirmed")),
        ("corp_NVDA", "corp_MSFT", "business_partner", 0.90, ev("public_record", "confirmed")),
        ("corp_NVDA", "corp_GOOG", "business_partner", 0.85, ev("public_record", "confirmed")),
        ("corp_NVDA", "corp_AMZN", "business_partner", 0.85, ev("public_record", "confirmed")),
        ("corp_NVDA", "corp_META", "business_partner", 0.85, ev("public_record", "confirmed")),
        # Cloud computing partnerships
        ("corp_MSFT", "corp_SNOW", "business_partner", 0.70, ev("public_record", "confirmed")),
        ("corp_MSFT", "corp_CRM", "business_partner", 0.60, ev("public_record", "confirmed")),
        ("corp_AMZN", "corp_SNOW", "business_partner", 0.70, ev("public_record", "confirmed")),
        ("corp_GOOG", "corp_DDOG", "business_partner", 0.60, ev("public_record", "confirmed")),
        # Automotive supply chain
        ("corp_TSLA", "corp_CATL", "business_partner", 0.85, ev("public_record", "confirmed")),
        ("corp_TSLA", "corp_373220", "business_partner", 0.70, ev("public_record", "confirmed")),
        ("corp_TM", "corp_6902", "business_partner", 0.90, ev("public_record", "confirmed")),
        ("corp_BYD", "corp_300750", "business_partner", 0.85, ev("public_record", "confirmed")),
        # Pharma partnerships
        ("corp_ROG", "corp_4519", "business_partner", 0.85, ev("public_record", "confirmed")),
        ("corp_AZN", "corp_4568", "business_partner", 0.80, ev("public_record", "confirmed")),
        # Trading houses → commodities
        ("corp_8058", "corp_8001", "business_partner", 0.60, ev("public_record", "confirmed")),
        ("corp_8031", "corp_8058", "business_partner", 0.60, ev("public_record", "confirmed")),
        # Industrial conglomerates
        ("corp_SIE", "corp_ABB", "business_partner", 0.50, ev("industry", "estimated")),
        ("corp_GE", "corp_SIE", "competitor", 0.70, ev("industry", "confirmed")),
        # SoftBank portfolio
        ("corp_9984", "corp_ARM", "co_investor", 0.95, ev("public_record", "confirmed")),
        # Berkshire portfolio companies
        ("corp_BRK", "corp_AAPL", "co_investor", 0.90, ev("sec_filing", "public_record")),
        ("corp_BRK", "corp_KO", "co_investor", 0.95, ev("sec_filing", "public_record")),
        ("corp_BRK", "corp_BAC", "co_investor", 0.85, ev("sec_filing", "public_record")),
        ("corp_BRK", "corp_AXP", "co_investor", 0.85, ev("sec_filing", "public_record")),
        ("corp_BRK", "corp_CVX", "co_investor", 0.80, ev("sec_filing", "public_record")),
        ("corp_BRK", "corp_OXY", "co_investor", 0.85, ev("sec_filing", "public_record")),
        ("corp_BRK", "corp_HPQ", "co_investor", 0.60, ev("sec_filing", "public_record")),
        # BlackRock top holdings
        ("corp_BLK", "corp_AAPL", "co_investor", 0.90, ev("sec_filing", "public_record")),
        ("corp_BLK", "corp_MSFT", "co_investor", 0.90, ev("sec_filing", "public_record")),
        ("corp_BLK", "corp_AMZN", "co_investor", 0.85, ev("sec_filing", "public_record")),
        ("corp_BLK", "corp_NVDA", "co_investor", 0.85, ev("sec_filing", "public_record")),
        ("corp_BLK", "corp_GOOG", "co_investor", 0.85, ev("sec_filing", "public_record")),
        ("corp_BLK", "corp_META", "co_investor", 0.80, ev("sec_filing", "public_record")),
        ("corp_BLK", "corp_JPM", "co_investor", 0.80, ev("sec_filing", "public_record")),
        # Vanguard (institutional) — represented via corp
        # Acquisition targets / relationships
        ("corp_MSFT", "corp_GOOG", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_MSFT", "corp_CRM", "competitor", 0.70, ev("industry", "confirmed")),
        # Alibaba / China tech ecosystem
        ("corp_BABA", "corp_SE", "co_investor", 0.60, ev("sec_filing", "public_record")),
        ("corp_TCEHY", "corp_JD", "co_investor", 0.70, ev("sec_filing", "public_record")),
        ("corp_TCEHY", "corp_SE", "co_investor", 0.65, ev("sec_filing", "public_record")),
        ("corp_TCEHY", "corp_SPOT", "co_investor", 0.40, ev("sec_filing", "public_record")),
    ]
    connections.extend(more_business)

    # ═══════════════════════════════════════════════
    # Additional political/diplomatic connections
    # ═══════════════════════════════════════════════
    more_diplomatic = [
        # NATO members → NATO
        ("gov_uk_starmer", "gov_nato_rutte", "diplomatic_relationship", 0.80, ev("public_record", "confirmed")),
        ("gov_france_macron", "gov_nato_rutte", "diplomatic_relationship", 0.80, ev("public_record", "confirmed")),
        ("gov_germany_scholz", "gov_nato_rutte", "diplomatic_relationship", 0.80, ev("public_record", "confirmed")),
        ("gov_poland_tusk", "gov_nato_rutte", "diplomatic_relationship", 0.80, ev("public_record", "confirmed")),
        ("gov_canada_carney", "gov_nato_rutte", "diplomatic_relationship", 0.75, ev("public_record", "confirmed")),
        ("gov_turkey_erdogan", "gov_nato_rutte", "diplomatic_relationship", 0.60, ev("public_record", "confirmed")),
        # EU members → EU Commission
        ("gov_italy_meloni", "gov_eu_vonderleyen", "diplomatic_relationship", 0.70, ev("public_record", "confirmed")),
        ("gov_spain_sanchez", "gov_eu_vonderleyen", "diplomatic_relationship", 0.75, ev("public_record", "confirmed")),
        ("gov_poland_tusk", "gov_eu_vonderleyen", "diplomatic_relationship", 0.80, ev("public_record", "confirmed")),
        ("gov_netherlands_schoof", "gov_eu_vonderleyen", "diplomatic_relationship", 0.75, ev("public_record", "confirmed")),
        # ASEAN relationships
        ("gov_singapore_wong", "gov_malaysia_anwar", "diplomatic_relationship", 0.75, ev("public_record", "confirmed")),
        ("gov_singapore_wong", "gov_indonesia_prabowo", "diplomatic_relationship", 0.70, ev("public_record", "confirmed")),
        ("gov_indonesia_prabowo", "gov_malaysia_anwar", "diplomatic_relationship", 0.65, ev("public_record", "confirmed")),
        ("gov_thailand_paetongtarn", "gov_vietnam_to_lam", "diplomatic_relationship", 0.60, ev("public_record", "confirmed")),
        ("gov_philippines_marcos", "gov_us_trump", "diplomatic_relationship", 0.60, ev("news_reports", "confirmed")),
        ("gov_philippines_marcos", "gov_china_xi", "diplomatic_relationship", 0.50, ev("news_reports", "confirmed")),
        # African Union relationships
        ("gov_nigeria_tinubu", "gov_south_africa_ramaphosa", "diplomatic_relationship", 0.60, ev("public_record", "confirmed")),
        ("gov_kenya_ruto", "gov_us_trump", "diplomatic_relationship", 0.55, ev("news_reports", "estimated")),
        ("gov_egypt_sisi", "gov_saudi_mbs", "diplomatic_relationship", 0.75, ev("public_record", "confirmed")),
        ("gov_egypt_sisi", "gov_uae_mbz", "diplomatic_relationship", 0.70, ev("public_record", "confirmed")),
        ("gov_morocco_mohammed", "gov_france_macron", "diplomatic_relationship", 0.65, ev("public_record", "confirmed")),
        ("gov_algeria_tebboune", "gov_russia_putin", "diplomatic_relationship", 0.60, ev("news_reports", "estimated")),
        # Latin America
        ("gov_brazil_lula", "gov_argentina_milei", "diplomatic_relationship", 0.30, ev("news_reports", "confirmed")),
        ("gov_colombia_petro", "gov_brazil_lula", "diplomatic_relationship", 0.70, ev("public_record", "confirmed")),
        ("gov_chile_boric", "gov_colombia_petro", "diplomatic_relationship", 0.65, ev("news_reports", "confirmed")),
        ("gov_mexico_sheinbaum", "gov_us_trump", "diplomatic_relationship", 0.40, ev("news_reports", "confirmed")),
        ("gov_venezuela_maduro", "gov_russia_putin", "diplomatic_relationship", 0.70, ev("public_record", "confirmed")),
        ("gov_venezuela_maduro", "gov_china_xi", "diplomatic_relationship", 0.60, ev("news_reports", "confirmed")),
        ("gov_cuba_diaz_canel", "gov_russia_putin", "diplomatic_relationship", 0.65, ev("public_record", "confirmed")),
        ("gov_cuba_diaz_canel", "gov_china_xi", "diplomatic_relationship", 0.60, ev("public_record", "confirmed")),
        # Central Asian
        ("gov_kazakhstan_tokayev", "gov_russia_putin", "diplomatic_relationship", 0.70, ev("public_record", "confirmed")),
        ("gov_kazakhstan_tokayev", "gov_china_xi", "diplomatic_relationship", 0.65, ev("public_record", "confirmed")),
        ("gov_uzbekistan_mirziyoyev", "gov_russia_putin", "diplomatic_relationship", 0.60, ev("public_record", "confirmed")),
        ("gov_uzbekistan_mirziyoyev", "gov_china_xi", "diplomatic_relationship", 0.60, ev("public_record", "confirmed")),
        # Japan alliances
        ("gov_japan_ishiba", "gov_south_korea_yoon", "diplomatic_relationship", 0.55, ev("news_reports", "confirmed")),
        ("gov_japan_ishiba", "gov_australia_albanese", "diplomatic_relationship", 0.70, ev("public_record", "confirmed")),
        ("gov_australia_albanese", "gov_newzealand_luxon", "diplomatic_relationship", 0.80, ev("public_record", "confirmed")),
        # Five Eyes intelligence alliance
        ("gov_us_trump", "gov_uk_starmer", "political_alliance", 0.70, ev("public_record", "confirmed")),
        ("gov_us_trump", "gov_australia_albanese", "political_alliance", 0.70, ev("public_record", "confirmed")),
        ("gov_us_trump", "gov_canada_carney", "political_alliance", 0.60, ev("public_record", "confirmed")),
        ("gov_uk_starmer", "gov_australia_albanese", "political_alliance", 0.70, ev("public_record", "confirmed")),
        # Gulf relationships
        ("gov_qatar_tamim", "gov_turkey_erdogan", "diplomatic_relationship", 0.75, ev("public_record", "confirmed")),
        ("gov_uae_mbz", "gov_india_modi", "diplomatic_relationship", 0.70, ev("public_record", "confirmed")),
        ("gov_saudi_mbs", "gov_egypt_sisi", "diplomatic_relationship", 0.75, ev("public_record", "confirmed")),
        ("gov_uae_mbz", "gov_israel_netanyahu", "diplomatic_relationship", 0.70, ev("public_record", "confirmed")),
        ("gov_bahrain_hamad", "gov_israel_netanyahu", "diplomatic_relationship", 0.65, ev("public_record", "confirmed")),
        # Authoritarian alignment
        ("gov_hungary_orban", "gov_russia_putin", "diplomatic_relationship", 0.65, ev("news_reports", "confirmed")),
        ("gov_hungary_orban", "gov_china_xi", "diplomatic_relationship", 0.50, ev("news_reports", "estimated")),
        ("gov_serbia_vucic", "gov_russia_putin", "diplomatic_relationship", 0.60, ev("news_reports", "confirmed")),
        ("gov_serbia_vucic", "gov_china_xi", "diplomatic_relationship", 0.55, ev("news_reports", "estimated")),
        ("gov_nicaragua_ortega", "gov_russia_putin", "diplomatic_relationship", 0.60, ev("news_reports", "confirmed")),
        ("gov_el_salvador_bukele", "gov_us_trump", "diplomatic_relationship", 0.70, ev("news_reports", "confirmed")),
        ("gov_myanmar_min_aung_hlaing", "gov_china_xi", "diplomatic_relationship", 0.55, ev("news_reports", "estimated")),
        ("gov_myanmar_min_aung_hlaing", "gov_russia_putin", "diplomatic_relationship", 0.50, ev("news_reports", "estimated")),
    ]
    connections.extend(more_diplomatic)

    # ═══════════════════════════════════════════════
    # Additional political donor connections
    # ═══════════════════════════════════════════════
    more_donors = [
        ("bil_jeff_yass", "gov_us_trump", "political_donor", 0.85, ev("opensecrets", "public_record")),
        ("bil_dan_gilbert", "gov_us_trump", "political_donor", 0.60, ev("opensecrets", "public_record")),
        ("bil_marc_andreessen", "gov_us_trump", "political_donor", 0.70, ev("news_reports", "confirmed")),
        ("bil_david_sacks", "gov_us_trump", "political_donor", 0.80, ev("news_reports", "confirmed")),
        ("bil_palmer_luckey", "gov_us_trump", "political_donor", 0.70, ev("opensecrets", "public_record")),
        ("bil_nelson_peltz", "gov_us_trump", "political_donor", 0.65, ev("opensecrets", "public_record")),
        ("bil_john_paulson", "gov_us_trump", "political_donor", 0.75, ev("opensecrets", "public_record")),
        ("bil_isaac_perlmutter", "gov_us_trump", "political_donor", 0.80, ev("opensecrets", "public_record")),
        ("bil_leon_cooperman", "gov_us_trump", "political_donor", 0.50, ev("opensecrets", "public_record")),
        # Democrat donors
        ("bil_reid_hoffman", "congress_pelosi", "political_donor", 0.55, ev("opensecrets", "public_record")),
        ("bil_eric_schmidt", "congress_pelosi", "political_donor", 0.50, ev("opensecrets", "public_record")),
        ("bil_michael_bloomberg2", "congress_pelosi", "political_donor", 0.65, ev("opensecrets", "public_record")),
        ("bil_vinod_khosla", "congress_pelosi", "political_donor", 0.45, ev("opensecrets", "public_record")),
        # Indian political donations
        ("bil_adani", "gov_india_modi", "political_donor", 0.60, ev("news_reports", "estimated")),
        ("bil_ambani", "gov_india_modi", "political_donor", 0.50, ev("news_reports", "estimated")),
        # Israeli political connections
        ("bil_eyal_ofer", "gov_israel_netanyahu", "political_donor", 0.40, ev("news_reports", "estimated")),
        # UK political
        ("bil_jim_ratcliffe", "gov_uk_starmer", "political_donor", 0.30, ev("news_reports", "estimated")),
        # Russia oligarch → Kremlin
        ("bil_arkady_rotenberg", "gov_russia_putin", "business_partner", 0.90, ev("news_reports", "confirmed")),
        ("bil_boris_rotenberg", "gov_russia_putin", "business_partner", 0.85, ev("news_reports", "confirmed")),
        ("bil_yuri_milner", "gov_russia_putin", "business_partner", 0.40, ev("news_reports", "estimated")),
        # Saudi → MBS
        ("bil_alwaleed", "gov_saudi_mbs", "business_partner", 0.50, ev("news_reports", "confirmed")),
        ("bil_al_rajhi", "gov_saudi_mbs", "business_partner", 0.40, ev("news_reports", "estimated")),
    ]
    connections.extend(more_donors)

    # ═══════════════════════════════════════════════
    # Additional billionaire-to-billionaire connections
    # ═══════════════════════════════════════════════
    more_bil_connections = [
        # PayPal Mafia
        ("bil_musk", "bil_thiel", "co_founder", 0.85, ev("public_record", "confirmed")),
        ("bil_thiel", "bil_reid_hoffman", "co_founder", 0.80, ev("public_record", "confirmed")),
        # Giving Pledge co-signers
        ("bil_gates", "bil_buffett", "co_founder", 0.90, ev("public_record", "confirmed")),
        ("bil_gates", "bil_bloomberg", "co_investor", 0.50, ev("news_reports", "confirmed")),
        ("bil_gates", "bil_dalio", "co_investor", 0.40, ev("news_reports", "estimated")),
        ("bil_gates", "bil_eric_schmidt", "co_investor", 0.50, ev("news_reports", "confirmed")),
        # Hedge fund rivalries
        ("bil_dalio", "bil_griffin", "competitor", 0.60, ev("industry", "confirmed")),
        ("bil_dalio", "bil_ackman", "competitor", 0.50, ev("industry", "confirmed")),
        ("bil_ackman", "bil_icahn", "competitor", 0.70, ev("news_reports", "confirmed")),
        ("bil_singer", "bil_icahn", "competitor", 0.60, ev("industry", "confirmed")),
        ("bil_soros", "bil_druckenmiller", "business_partner", 0.80, ev("public_record", "confirmed")),
        ("bil_simons_jim", "bil_robert_mercer", "business_partner", 0.90, ev("public_record", "confirmed")),
        # VC network
        ("bil_marc_andreessen", "bil_reid_hoffman", "co_investor", 0.75, ev("news_reports", "confirmed")),
        ("bil_marc_andreessen", "bil_vinod_khosla", "co_investor", 0.60, ev("news_reports", "confirmed")),
        ("bil_john_doerr", "bil_vinod_khosla", "business_partner", 0.70, ev("public_record", "confirmed")),
        ("bil_marc_andreessen", "bil_john_doerr", "co_investor", 0.55, ev("news_reports", "estimated")),
        ("bil_thiel", "bil_marc_andreessen", "co_investor", 0.70, ev("news_reports", "confirmed")),
        # Tech founders
        ("bil_musk", "bil_altman", "competitor", 0.80, ev("news_reports", "confirmed")),
        ("bil_gates", "bil_altman", "co_investor", 0.60, ev("news_reports", "confirmed")),
        ("bil_page", "bil_brin", "co_founder", 0.99, ev("public_record", "confirmed")),
        ("bil_page", "bil_eric_schmidt", "business_partner", 0.85, ev("public_record", "confirmed")),
        ("bil_brin", "bil_eric_schmidt", "business_partner", 0.85, ev("public_record", "confirmed")),
        # Luxury moguls
        ("bil_arnault", "bil_pinault", "competitor", 0.90, ev("industry", "confirmed")),
        ("bil_arnault", "bil_bettencourt", "business_partner", 0.40, ev("news_reports", "estimated")),
        # Indian tycoon connections
        ("bil_ambani", "bil_shiv_nadar", "business_partner", 0.30, ev("news_reports", "estimated")),
        ("bil_adani", "bil_ambani", "competitor", 0.80, ev("news_reports", "confirmed")),
        ("bil_bajaj_family", "bil_kumar_birla", "business_partner", 0.40, ev("news_reports", "estimated")),
        # 3G Capital trio
        ("bil_jorge_lemann", "bil_marcel_herrmann", "co_founder", 0.95, ev("public_record", "confirmed")),
        ("bil_jorge_lemann", "bil_carlos_alberto_sicupira", "co_founder", 0.95, ev("public_record", "confirmed")),
        ("bil_marcel_herrmann", "bil_carlos_alberto_sicupira", "co_founder", 0.95, ev("public_record", "confirmed")),
        # Apollo trio
        ("bil_leon_black", "bil_marc_rowan", "co_founder", 0.90, ev("public_record", "confirmed")),
        ("bil_leon_black", "bil_josh_harris", "co_founder", 0.90, ev("public_record", "confirmed")),
        ("bil_marc_rowan", "bil_josh_harris", "co_founder", 0.90, ev("public_record", "confirmed")),
        # Danaher brothers
        ("bil_mitchell_rales", "bil_steven_rales", "family", 0.99, ev("public_record", "confirmed")),
        # Gores brothers
        ("bil_tom_gores", "bil_alec_gores", "family", 0.99, ev("public_record", "confirmed")),
        # Collison brothers
        ("bil_patrick_collison", "bil_john_collison", "co_founder", 0.99, ev("public_record", "confirmed")),
        # Sports owners
        ("bil_robert_kraft", "bil_jerry_jones", "business_partner", 0.50, ev("news_reports", "confirmed")),
        ("bil_stan_kroenke", "bil_jerry_jones", "business_partner", 0.40, ev("news_reports", "estimated")),
        # Garcia family
        ("bil_ernest_garcia_ii", "bil_ernest_garcia_iii", "family", 0.99, ev("public_record", "confirmed")),
        # Japanese/Korean conglomerates
        ("bil_jay_y_lee", "bil_lee_jae_yong", "family", 0.99, ev("public_record", "confirmed")),
        # Charles/James Dolan
        ("bil_charles_dolan", "bil_james_dolan", "family", 0.99, ev("public_record", "confirmed")),
        # Uber co-founders
        ("bil_travis_kalanick", "bil_garrett_camp", "co_founder", 0.95, ev("public_record", "confirmed")),
        # Wertheimer brothers
        ("bil_wertheimer_alain", "bil_wertheimer_gerard", "co_founder", 0.95, ev("public_record", "confirmed")),
        # Hinduja family
        ("bil_gopichand_hinduja", "bil_hinduja_family", "family", 0.99, ev("public_record", "confirmed")),
        # Russian billionaire cliques
        ("bil_mikhail_fridman", "bil_pyotr_aven", "business_partner", 0.90, ev("public_record", "confirmed")),
        ("bil_mikhail_fridman", "bil_german_khan", "business_partner", 0.90, ev("public_record", "confirmed")),
        ("bil_pyotr_aven", "bil_german_khan", "business_partner", 0.90, ev("public_record", "confirmed")),
        ("bil_arkady_rotenberg", "bil_boris_rotenberg", "family", 0.99, ev("public_record", "confirmed")),
        # Tech billionaire friendships / investments
        ("bil_bezos", "bil_altman", "co_investor", 0.60, ev("news_reports", "confirmed")),
        ("bil_thiel", "bil_musk", "co_investor", 0.70, ev("news_reports", "confirmed")),
        ("bil_son", "bil_ma", "co_investor", 0.80, ev("sec_filing", "public_record")),
        ("bil_son", "bil_altman", "co_investor", 0.60, ev("news_reports", "confirmed")),
        # Red Bull family
        ("bil_chalerm_yoovidhya", "bil_mark_mateschitz", "business_partner", 0.90, ev("public_record", "confirmed")),
        # PE mogul connections
        ("bil_schwarzman", "bil_orlando_bravo", "competitor", 0.60, ev("industry", "confirmed")),
        ("bil_schwarzman", "bil_robert_smith", "competitor", 0.55, ev("industry", "confirmed")),
        ("bil_kravis", "bil_schwarzman", "competitor", 0.70, ev("industry", "confirmed")),
        # Finance + Tech overlap
        ("bil_griffin", "bil_changpeng_zhao", "competitor", 0.40, ev("news_reports", "estimated")),
        ("bil_michael_saylor", "bil_changpeng_zhao", "business_partner", 0.30, ev("news_reports", "estimated")),
    ]
    connections.extend(more_bil_connections)

    # ═══════════════════════════════════════════════
    # Additional regulatory connections
    # ═══════════════════════════════════════════════
    more_regulatory = [
        ("gov_china_xi", "corp_1810", "regulatory_relationship", 0.80, ev("public_record", "confirmed")),
        ("gov_china_xi", "corp_BYD", "regulatory_relationship", 0.70, ev("news_reports", "confirmed")),
        ("gov_china_xi", "corp_CATL", "regulatory_relationship", 0.70, ev("news_reports", "confirmed")),
        ("gov_us_trump", "corp_TSLA", "regulatory_relationship", 0.80, ev("news_reports", "confirmed")),
        ("gov_us_trump", "corp_AAPL", "regulatory_relationship", 0.70, ev("news_reports", "confirmed")),
        ("gov_us_trump", "corp_GOOG", "regulatory_relationship", 0.75, ev("news_reports", "confirmed")),
        ("gov_us_trump", "corp_META", "regulatory_relationship", 0.70, ev("news_reports", "confirmed")),
        ("gov_us_trump", "corp_AMZN", "regulatory_relationship", 0.65, ev("news_reports", "confirmed")),
        ("gov_us_trump", "corp_NVDA", "regulatory_relationship", 0.75, ev("news_reports", "confirmed")),
        ("gov_india_modi", "corp_RELIANCE", "regulatory_relationship", 0.75, ev("news_reports", "confirmed")),
        ("gov_india_modi", "corp_ADANIENT", "regulatory_relationship", 0.80, ev("news_reports", "confirmed")),
        ("gov_india_modi", "corp_TCS", "regulatory_relationship", 0.50, ev("public_record", "confirmed")),
        ("gov_japan_ishiba", "corp_TM", "regulatory_relationship", 0.60, ev("public_record", "confirmed")),
        ("gov_south_korea_yoon", "corp_samsung", "regulatory_relationship", 0.75, ev("public_record", "confirmed")),
        ("gov_south_korea_yoon", "corp_000660", "regulatory_relationship", 0.65, ev("public_record", "confirmed")),
        ("gov_france_macron", "corp_MC", "regulatory_relationship", 0.50, ev("public_record", "confirmed")),
        ("gov_france_macron", "corp_TTE", "regulatory_relationship", 0.65, ev("public_record", "confirmed")),
        ("gov_germany_scholz", "corp_VOW", "regulatory_relationship", 0.65, ev("public_record", "confirmed")),
        ("gov_germany_scholz", "corp_SIE", "regulatory_relationship", 0.55, ev("public_record", "confirmed")),
        ("gov_uk_starmer", "corp_SHEL", "regulatory_relationship", 0.60, ev("public_record", "confirmed")),
        ("gov_uk_starmer", "corp_BP", "regulatory_relationship", 0.60, ev("public_record", "confirmed")),
        ("gov_saudi_mbs", "corp_ARAMCO", "regulatory_relationship", 0.95, ev("public_record", "confirmed")),
        ("gov_uae_mbz", "corp_FAB", "regulatory_relationship", 0.70, ev("public_record", "confirmed")),
        ("gov_brazil_lula", "corp_PBR", "regulatory_relationship", 0.80, ev("public_record", "confirmed")),
        ("gov_brazil_lula", "corp_VALE", "regulatory_relationship", 0.70, ev("public_record", "confirmed")),
        ("gov_mexico_sheinbaum", "corp_AMX", "regulatory_relationship", 0.55, ev("public_record", "confirmed")),
        ("gov_australia_albanese", "corp_BHP", "regulatory_relationship", 0.60, ev("public_record", "confirmed")),
        ("gov_australia_albanese", "corp_RIO", "regulatory_relationship", 0.55, ev("public_record", "confirmed")),
        ("gov_nigeria_tinubu", "corp_DANGOTE_CEMENT", "regulatory_relationship", 0.60, ev("news_reports", "confirmed")),
    ]
    connections.extend(more_regulatory)

    # ═══════════════════════════════════════════════
    # Leader → Company: government contracts, strategic ties
    # ═══════════════════════════════════════════════
    defense_gov = [
        ("gov_us_trump", "corp_LMT", "regulatory_relationship", 0.80, ev("public_record", "confirmed")),
        ("gov_us_trump", "corp_RTX", "regulatory_relationship", 0.80, ev("public_record", "confirmed")),
        ("gov_us_trump", "corp_NOC", "regulatory_relationship", 0.75, ev("public_record", "confirmed")),
        ("gov_us_trump", "corp_GD", "regulatory_relationship", 0.75, ev("public_record", "confirmed")),
        ("gov_us_trump", "corp_BA", "regulatory_relationship", 0.75, ev("public_record", "confirmed")),
        ("gov_uk_starmer", "corp_BA_L", "regulatory_relationship", 0.70, ev("public_record", "confirmed")),
        ("gov_uk_starmer", "corp_RR", "regulatory_relationship", 0.70, ev("public_record", "confirmed")),
        ("gov_france_macron", "corp_EAF", "regulatory_relationship", 0.75, ev("public_record", "confirmed")),
        ("gov_israel_netanyahu", "corp_LMT", "business_partner", 0.65, ev("news_reports", "confirmed")),
        ("gov_saudi_mbs", "corp_LMT", "business_partner", 0.70, ev("news_reports", "confirmed")),
        ("gov_saudi_mbs", "corp_BA", "business_partner", 0.70, ev("news_reports", "confirmed")),
        ("gov_uae_mbz", "corp_LMT", "business_partner", 0.65, ev("news_reports", "confirmed")),
        ("gov_india_modi", "corp_LMT", "business_partner", 0.50, ev("news_reports", "estimated")),
        ("gov_taiwan_lai", "corp_TSM", "regulatory_relationship", 0.90, ev("public_record", "confirmed")),
        ("gov_taiwan_lai", "corp_2317", "regulatory_relationship", 0.70, ev("public_record", "confirmed")),
    ]
    connections.extend(defense_gov)

    # ═══════════════════════════════════════════════
    # Additional family connections
    # ═══════════════════════════════════════════════
    more_family = [
        ("bil_rupert_murdoch", "bil_donald_trump", "business_partner", 0.65, ev("news_reports", "confirmed")),
        ("bil_robert_kraft", "bil_donald_trump", "business_partner", 0.60, ev("news_reports", "confirmed")),
        ("bil_lee_kun_hee_estate", "bil_jay_y_lee", "family", 0.99, ev("public_record", "confirmed")),
        ("bil_chung_mong_koo", "bil_chung_euisun", "family", 0.99, ev("public_record", "confirmed")),
        ("bil_del_vecchio_estate", "bil_miuccia_prada", "competitor", 0.40, ev("industry", "confirmed")),
        ("bil_arnault", "bil_hermes_family", "competitor", 0.70, ev("news_reports", "confirmed")),
        ("bil_arnault", "bil_pinault", "competitor", 0.90, ev("industry", "confirmed")),
        ("bil_sumner_redstone_estate", "corp_paramount", "co_investor", 0.90, ev("public_record", "confirmed")),
        ("bil_john_henry", "corp_boston_red_sox", "employed_by", 0.95, ev("public_record", "confirmed")),
        ("bil_shahid_khan", "corp_jaguars", "employed_by", 0.95, ev("public_record", "confirmed")),
        ("bil_steve_ballmer2", "corp_la_clippers", "employed_by", 0.95, ev("public_record", "confirmed")),
        ("bil_joe_tsai", "corp_brooklyn_nets", "employed_by", 0.95, ev("public_record", "confirmed")),
        ("bil_tilman_fertitta", "corp_houston_rockets", "employed_by", 0.95, ev("public_record", "confirmed")),
    ]
    connections.extend(more_family)

    # ═══════════════════════════════════════════════
    # Additional corporate lobbying relationships
    # ═══════════════════════════════════════════════
    more_lobbying = [
        ("corp_JPM", "gov_us_trump", "lobbies_for", 0.65, ev("opensecrets", "public_record")),
        ("corp_GS", "gov_us_trump", "lobbies_for", 0.65, ev("opensecrets", "public_record")),
        ("corp_BAC", "gov_us_trump", "lobbies_for", 0.60, ev("opensecrets", "public_record")),
        ("corp_BLK", "gov_us_trump", "lobbies_for", 0.55, ev("opensecrets", "public_record")),
        ("corp_XOM", "gov_us_trump", "lobbies_for", 0.70, ev("opensecrets", "public_record")),
        ("corp_CVX", "gov_us_trump", "lobbies_for", 0.70, ev("opensecrets", "public_record")),
        ("corp_LMT", "gov_us_trump", "lobbies_for", 0.70, ev("opensecrets", "public_record")),
        ("corp_RTX", "gov_us_trump", "lobbies_for", 0.65, ev("opensecrets", "public_record")),
        ("corp_BA", "gov_us_trump", "lobbies_for", 0.65, ev("opensecrets", "public_record")),
        ("corp_PFE", "gov_us_trump", "lobbies_for", 0.60, ev("opensecrets", "public_record")),
        ("corp_UNH", "gov_us_trump", "lobbies_for", 0.65, ev("opensecrets", "public_record")),
        ("corp_NVDA", "gov_us_trump", "lobbies_for", 0.55, ev("opensecrets", "public_record")),
        ("corp_TSLA", "gov_us_trump", "lobbies_for", 0.50, ev("opensecrets", "public_record")),
        ("corp_T", "gov_us_trump", "lobbies_for", 0.55, ev("opensecrets", "public_record")),
        ("corp_VZ", "gov_us_trump", "lobbies_for", 0.55, ev("opensecrets", "public_record")),
        ("corp_CMCSA", "gov_us_trump", "lobbies_for", 0.55, ev("opensecrets", "public_record")),
        ("corp_WMT", "gov_us_trump", "lobbies_for", 0.50, ev("opensecrets", "public_record")),
        ("corp_DIS", "gov_us_trump", "lobbies_for", 0.50, ev("opensecrets", "public_record")),
    ]
    connections.extend(more_lobbying)

    # ═══════════════════════════════════════════════
    # Additional shareholder / co_investor connections
    # ═══════════════════════════════════════════════
    more_shareholders = [
        ("bil_jay_y_lee", "corp_samsung", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_chung_euisun", "corp_hyundai", "co_investor", 0.90, ev("public_record", "confirmed")),
        ("bil_koo_kwang_mo", "corp_051910", "co_investor", 0.85, ev("public_record", "confirmed")),
        ("bil_shin_dong_bin", "corp_lotte", "co_investor", 0.85, ev("public_record", "confirmed")),
        ("bil_chey_tae_won", "corp_000660", "co_investor", 0.80, ev("public_record", "confirmed")),
        ("bil_terry_gou", "corp_2317", "co_investor", 0.85, ev("public_record", "confirmed")),
        ("bil_morris_chang", "corp_TSM", "co_investor", 0.70, ev("public_record", "confirmed")),
        ("bil_li_ka_shing", "corp_ck_hutchison", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_henry_cheng", "corp_chow_tai_fook", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_lui_che_woo", "corp_galaxy_ent", "co_investor", 0.90, ev("public_record", "confirmed")),
        ("bil_robert_kuok", "corp_kuok_group", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_pham_nhat_vuong", "corp_vingroup", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_slim", "corp_AMX", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_ricardo_salinas", "corp_tv_azteca", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_iris_fontbona", "corp_antofagasta", "co_investor", 0.90, ev("public_record", "confirmed")),
        ("bil_aliko_dangote", "corp_DANGOTE_CEMENT", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_johann_rupert", "corp_richemont", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_dhanin_chearavanont", "corp_cp_group", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_gina_rinehart", "corp_hancock_prospecting", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_andrew_forrest", "corp_FMG", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_mike_cannon_brookes", "corp_TEAM", "co_investor", 0.90, ev("sec_filing", "public_record")),
        ("bil_scott_farquhar", "corp_TEAM", "co_investor", 0.90, ev("sec_filing", "public_record")),
        ("bil_tobi_lutke", "corp_SHOP", "co_investor", 0.90, ev("sec_filing", "public_record")),
        ("bil_chip_wilson", "corp_LULU", "co_investor", 0.70, ev("public_record", "confirmed")),
        ("bil_jimmy_pattison", "corp_pattison_group", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_dieter_schwarz", "corp_schwarz_group", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_stefan_quandt", "corp_BMW", "co_investor", 0.85, ev("public_record", "confirmed")),
        ("bil_susanne_klatten", "corp_BMW", "co_investor", 0.85, ev("public_record", "confirmed")),
        ("bil_giovanni_ferrero", "corp_ferrero_spa", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_emmanuel_besnier", "corp_lactalis", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_gerard_mulliez", "corp_auchan", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_dassault_family", "corp_dassault", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_rodolphe_saade", "corp_cma_cgm", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_gianluigi_aponte", "corp_msc_shipping", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_zeng_yuqun", "corp_CATL", "co_investor", 0.90, ev("public_record", "confirmed")),
        ("bil_wang_chuanfu", "corp_BYD", "co_investor", 0.90, ev("public_record", "confirmed")),
        ("bil_lei_jun", "corp_1810", "co_investor", 0.90, ev("public_record", "confirmed")),
        ("bil_ma_huateng", "corp_TCEHY", "co_investor", 0.85, ev("public_record", "confirmed")),
        ("bil_colin_huang", "corp_PDD", "co_investor", 0.85, ev("public_record", "confirmed")),
        ("bil_zhangyiming", "corp_bytedance", "co_investor", 0.90, ev("public_record", "confirmed")),
        ("bil_wang_xing", "corp_MEITUAN", "co_investor", 0.90, ev("public_record", "confirmed")),
        ("bil_robin_li", "corp_BIDU", "co_investor", 0.85, ev("public_record", "confirmed")),
        ("bil_liu_qiangdong", "corp_JD", "co_investor", 0.85, ev("public_record", "confirmed")),
        ("bil_ding_shizhong", "corp_anta_sports", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_li_ning", "corp_li_ning_brand", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_jeff_yass", "corp_susquehanna", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_thomas_peterffy", "corp_IBKR", "co_investor", 0.95, ev("sec_filing", "public_record")),
        ("bil_david_tepper", "corp_appaloosa", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_israel_englander", "corp_millennium", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_david_shaw", "corp_de_shaw", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_michael_platt", "corp_bluecrest", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_alan_howard", "corp_brevan_howard", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_chris_hohn", "corp_tci_fund", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_chase_coleman", "corp_tiger_global", "co_investor", 0.95, ev("public_record", "confirmed")),
    ]
    connections.extend(more_shareholders)

    # ═══════════════════════════════════════════════
    # BULK: Every company gets CEO employed_by link
    # Generated from company data: CEO → corp
    # ═══════════════════════════════════════════════
    bulk_ceo_employed = [
        # S&P 500 CEOs → Their companies (not already mapped above)
        ("bil_dimon", "corp_JPM", 0.99),
        ("bil_buffett", "corp_BRK", 0.99),
        ("bil_dan_gilbert", "corp_rocket_mortgage", 0.95),
        ("bil_john_malone", "corp_liberty_media", 0.90),
        ("bil_marc_benioff", "corp_CRM", 0.95),
        ("bil_abigail_johnson", "corp_fidelity", 0.99),
        ("bil_brian_chesky", "corp_ABNB", 0.95),
        ("bil_palmer_luckey", "corp_anduril", 0.95),
        ("bil_altman", "corp_openai", 0.95),
        ("bil_patrick_collison", "corp_stripe", 0.95),
        ("bil_changpeng_zhao", "corp_binance", 0.80),
        ("bil_ren_zhengfei", "corp_huawei", 0.95),
        ("bil_zhangyiming", "corp_bytedance", 0.70),
        ("bil_musk", "corp_spacex", 0.95),
        ("bil_koch_charles", "corp_koch_industries", 0.95),
        ("bil_bloomberg", "corp_bloomberg_lp", 0.90),
        ("bil_john_mars", "corp_mars_inc", 0.70),
    ]
    for (a, b, s) in bulk_ceo_employed:
        connections.append((a, b, "employed_by", s, ev("public_record", "confirmed")))

    # ═══════════════════════════════════════════════
    # BULK: Cross-industry supply chain relationships
    # ═══════════════════════════════════════════════
    supply_chain = [
        # Semiconductor equipment → Foundries
        ("corp_ASML", "corp_TSM", "business_partner", 0.95, ev("public_record", "confirmed")),
        ("corp_ASML", "corp_samsung", "business_partner", 0.85, ev("public_record", "confirmed")),
        ("corp_ASML", "corp_INTC", "business_partner", 0.85, ev("public_record", "confirmed")),
        ("corp_AMAT", "corp_TSM", "business_partner", 0.85, ev("public_record", "confirmed")),
        ("corp_AMAT", "corp_samsung", "business_partner", 0.80, ev("public_record", "confirmed")),
        ("corp_LRCX", "corp_TSM", "business_partner", 0.85, ev("public_record", "confirmed")),
        ("corp_KLAC", "corp_TSM", "business_partner", 0.80, ev("public_record", "confirmed")),
        ("corp_8035", "corp_TSM", "business_partner", 0.80, ev("public_record", "confirmed")),
        # EDA → Chip designers
        ("corp_SNPS", "corp_NVDA", "business_partner", 0.80, ev("public_record", "confirmed")),
        ("corp_SNPS", "corp_AMD", "business_partner", 0.75, ev("public_record", "confirmed")),
        ("corp_SNPS", "corp_QCOM", "business_partner", 0.75, ev("public_record", "confirmed")),
        ("corp_CDNS", "corp_NVDA", "business_partner", 0.75, ev("public_record", "confirmed")),
        ("corp_CDNS", "corp_AMD", "business_partner", 0.70, ev("public_record", "confirmed")),
        ("corp_SNPS", "corp_ARM", "business_partner", 0.80, ev("public_record", "confirmed")),
        ("corp_ARM", "corp_AAPL", "business_partner", 0.85, ev("public_record", "confirmed")),
        ("corp_ARM", "corp_QCOM", "business_partner", 0.85, ev("public_record", "confirmed")),
        ("corp_ARM", "corp_samsung", "business_partner", 0.75, ev("public_record", "confirmed")),
        ("corp_ARM", "corp_2454", "business_partner", 0.80, ev("public_record", "confirmed")),
        # Cloud partnerships
        ("corp_MSFT", "corp_NVDA", "business_partner", 0.90, ev("public_record", "confirmed")),
        ("corp_GOOG", "corp_NVDA", "business_partner", 0.85, ev("public_record", "confirmed")),
        ("corp_AMZN", "corp_NVDA", "business_partner", 0.85, ev("public_record", "confirmed")),
        ("corp_META", "corp_NVDA", "business_partner", 0.85, ev("public_record", "confirmed")),
        ("corp_ORCL", "corp_NVDA", "business_partner", 0.75, ev("public_record", "confirmed")),
        # Microsoft + OpenAI
        ("corp_MSFT", "corp_openai", "co_investor", 0.95, ev("public_record", "confirmed")),
        # Tech → Cloud (SaaS on cloud)
        ("corp_SNOW", "corp_AMZN", "business_partner", 0.75, ev("public_record", "confirmed")),
        ("corp_SNOW", "corp_GOOG", "business_partner", 0.65, ev("public_record", "confirmed")),
        ("corp_DDOG", "corp_AMZN", "business_partner", 0.70, ev("public_record", "confirmed")),
        ("corp_CRM", "corp_AMZN", "business_partner", 0.65, ev("public_record", "confirmed")),
        # Automotive → Battery
        ("corp_TM", "corp_373220", "business_partner", 0.70, ev("public_record", "confirmed")),
        ("corp_GM", "corp_373220", "business_partner", 0.65, ev("news_reports", "confirmed")),
        ("corp_STLA", "corp_373220", "business_partner", 0.50, ev("news_reports", "estimated")),
        ("corp_F", "corp_CATL", "business_partner", 0.60, ev("news_reports", "confirmed")),
        ("corp_VOW", "corp_CATL", "business_partner", 0.70, ev("news_reports", "confirmed")),
        ("corp_BMW", "corp_CATL", "business_partner", 0.65, ev("news_reports", "confirmed")),
        ("corp_MBG", "corp_CATL", "business_partner", 0.60, ev("news_reports", "confirmed")),
        # Pharma / biotech partnerships
        ("corp_LLY", "corp_AMGN", "competitor", 0.60, ev("industry", "confirmed")),
        ("corp_MRK", "corp_AZN", "competitor", 0.55, ev("industry", "confirmed")),
        ("corp_JNJ", "corp_ABBV", "competitor", 0.65, ev("industry", "confirmed")),
        ("corp_NOVN", "corp_SAN_FR", "competitor", 0.60, ev("industry", "confirmed")),
        # Airlines
        ("corp_DAL", "corp_UAL", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_DAL", "corp_AAL2", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_DAL", "corp_LUV", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_UAL", "corp_AAL2", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_LUV", "corp_AAL2", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_LHA", "corp_EAF", "business_partner", 0.50, ev("industry", "estimated")),
        # Japanese trading houses
        ("corp_8001", "corp_8058", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_8001", "corp_8031", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_8058", "corp_8031", "competitor", 0.70, ev("industry", "confirmed")),
        # Luxury conglomerate → brands
        ("corp_MC", "corp_OR", "competitor", 0.50, ev("industry", "confirmed")),
        # Apparel
        ("corp_LULU", "corp_NKE", "competitor", 0.65, ev("industry", "confirmed")),
        ("corp_DECK", "corp_NKE", "competitor", 0.50, ev("industry", "confirmed")),
        # Food delivery
        ("corp_DASH", "corp_UBER", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_MEITUAN", "corp_BABA", "competitor", 0.60, ev("industry", "confirmed")),
        # E-commerce
        ("corp_SHOP", "corp_AMZN", "competitor", 0.50, ev("industry", "confirmed")),
        ("corp_EBAY", "corp_AMZN", "competitor", 0.60, ev("industry", "confirmed")),
        ("corp_ETSY", "corp_AMZN", "competitor", 0.45, ev("industry", "confirmed")),
        ("corp_CPNG", "corp_BABA", "competitor", 0.50, ev("industry", "confirmed")),
        ("corp_MELI", "corp_BABA", "competitor", 0.40, ev("industry", "confirmed")),
        ("corp_shein", "corp_ITX", "competitor", 0.65, ev("industry", "confirmed")),
        # Gaming
        ("corp_7974", "corp_6758", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_7974", "corp_MSFT", "competitor", 0.55, ev("industry", "confirmed")),
        ("corp_RBLX", "corp_U", "competitor", 0.65, ev("industry", "confirmed")),
        ("corp_epic_games", "corp_valve", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_epic_games", "corp_U", "competitor", 0.60, ev("industry", "confirmed")),
        # Streaming video
        ("corp_NFLX", "corp_DIS", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_NFLX", "corp_6758", "competitor", 0.40, ev("industry", "confirmed")),
        ("corp_IQ", "corp_BILI", "competitor", 0.75, ev("industry", "confirmed")),
        # Ride-hailing
        ("corp_UBER", "corp_LYFT", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_UBER", "corp_GRAB", "competitor", 0.60, ev("industry", "confirmed")),
        ("corp_GRAB", "corp_SE", "competitor", 0.75, ev("industry", "confirmed")),
        # Crypto exchanges
        ("corp_COIN", "corp_binance", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_COIN", "corp_kraken", "competitor", 0.75, ev("industry", "confirmed")),
        ("corp_binance", "corp_kraken", "competitor", 0.70, ev("industry", "confirmed")),
        # Australian banks
        ("corp_CBA", "corp_WBC", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_CBA", "corp_ANZ", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_CBA", "corp_NAB", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_WBC", "corp_ANZ", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_NAB", "corp_WBC", "competitor", 0.80, ev("industry", "confirmed")),
        # Australian mining
        ("corp_BHP", "corp_FMG", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_RIO", "corp_FMG", "competitor", 0.75, ev("industry", "confirmed")),
        # Brazilian banks
        ("corp_ITUB", "corp_B3SA", "business_partner", 0.50, ev("industry", "estimated")),
        ("corp_ITUB", "corp_NU", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_PBR", "corp_VALE", "competitor", 0.30, ev("industry", "confirmed")),
        # Mexican companies
        ("corp_AMX", "corp_FEMSA", "competitor", 0.30, ev("industry", "confirmed")),
        ("corp_CEMEX", "corp_VMC", "competitor", 0.60, ev("industry", "confirmed")),
        # Oil service
        ("corp_SLB", "corp_HAL", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_SLB", "corp_BKR", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_HAL", "corp_BKR", "competitor", 0.85, ev("industry", "confirmed")),
        # Midstream
        ("corp_WMB", "corp_KMI", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_WMB", "corp_OKE", "competitor", 0.75, ev("industry", "confirmed")),
        ("corp_KMI", "corp_EPD", "competitor", 0.75, ev("industry", "confirmed")),
        ("corp_ET", "corp_EPD", "competitor", 0.80, ev("industry", "confirmed")),
        # Russian energy → government
        ("corp_GAZP", "gov_russia_putin", "regulatory_relationship", 0.95, ev("public_record", "confirmed")),
        ("corp_ROSN", "gov_russia_putin", "regulatory_relationship", 0.95, ev("public_record", "confirmed")),
        ("corp_SBGS", "gov_russia_putin", "regulatory_relationship", 0.80, ev("public_record", "confirmed")),
        ("corp_LKOH", "gov_russia_putin", "regulatory_relationship", 0.70, ev("public_record", "confirmed")),
        ("corp_GMKN", "gov_russia_putin", "regulatory_relationship", 0.65, ev("public_record", "confirmed")),
        # Russian billionaire → company
        ("bil_vladimir_potanin", "corp_GMKN", "co_investor", 0.90, ev("public_record", "confirmed")),
        ("bil_vladimir_lisin", "corp_NLMK", "co_investor", 0.90, ev("public_record", "confirmed")),
        ("bil_vagit_alekperov", "corp_LKOH", "co_investor", 0.85, ev("public_record", "confirmed")),
        ("bil_leonid_mikhelson", "corp_novatek", "co_investor", 0.90, ev("public_record", "confirmed")),
        ("bil_alexey_mordashov", "corp_severstal", "co_investor", 0.90, ev("public_record", "confirmed")),
        # UAE/Saudi billionaire → company
        ("bil_hussain_sajwani", "corp_damac", "co_investor", 0.95, ev("public_record", "confirmed")),
        # Private company connections
        ("bil_musk", "corp_spacex", "co_founder", 0.95, ev("public_record", "confirmed")),
        ("bil_musk", "corp_spacex", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("corp_spacex", "corp_TSLA", "business_partner", 0.40, ev("news_reports", "estimated")),
        ("corp_spacex", "gov_us_trump", "business_partner", 0.70, ev("news_reports", "confirmed")),
        ("corp_anduril", "gov_us_trump", "business_partner", 0.60, ev("news_reports", "confirmed")),
        ("corp_openai", "corp_MSFT", "business_partner", 0.95, ev("public_record", "confirmed")),
        ("corp_bytedance", "corp_tiktok_us", "co_investor", 0.95, ev("public_record", "confirmed")),
        # Brookfield investments
        ("corp_BAM", "corp_BN", "business_partner", 0.95, ev("public_record", "confirmed")),
    ]
    connections.extend(supply_chain)

    # ═══════════════════════════════════════════════
    # BULK: Additional CEO → company for private cos
    # ═══════════════════════════════════════════════
    private_ceo = [
        ("bil_musk", "corp_spacex", "employed_by", 0.95, ev("public_record", "confirmed")),
        ("bil_koch_charles", "corp_koch_industries", "employed_by", 0.95, ev("public_record", "confirmed")),
        ("bil_john_mars", "corp_mars_inc", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_jacqueline_mars", "corp_mars_inc", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_wertheimer_alain", "corp_chanel", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_wertheimer_gerard", "corp_chanel", "co_investor", 0.95, ev("public_record", "confirmed")),
        ("bil_ren_zhengfei", "corp_huawei", "co_founder", 0.95, ev("public_record", "confirmed")),
        ("bil_tim_sweeney", "corp_epic_games", "co_founder", 0.95, ev("public_record", "confirmed")),
        ("bil_gabe_newell", "corp_valve", "co_founder", 0.95, ev("public_record", "confirmed")),
        ("bil_zhangyiming", "corp_bytedance", "co_founder", 0.95, ev("public_record", "confirmed")),
    ]
    connections.extend(private_ceo)

    # ═══════════════════════════════════════════════
    # BULK: More cross-sector connections
    # ═══════════════════════════════════════════════
    cross_sector = [
        # Payments ecosystem
        ("corp_V", "corp_AAPL", "business_partner", 0.80, ev("public_record", "confirmed")),
        ("corp_MA", "corp_AAPL", "business_partner", 0.75, ev("public_record", "confirmed")),
        ("corp_V", "corp_JPM", "business_partner", 0.85, ev("public_record", "confirmed")),
        ("corp_MA", "corp_JPM", "business_partner", 0.85, ev("public_record", "confirmed")),
        ("corp_V", "corp_BAC", "business_partner", 0.80, ev("public_record", "confirmed")),
        ("corp_MA", "corp_BAC", "business_partner", 0.80, ev("public_record", "confirmed")),
        ("corp_V", "corp_COF", "business_partner", 0.80, ev("public_record", "confirmed")),
        ("corp_V", "corp_WMT", "business_partner", 0.70, ev("public_record", "confirmed")),
        ("corp_PYPL", "corp_EBAY", "business_partner", 0.50, ev("public_record", "confirmed")),
        # Health insurance → Hospitals
        ("corp_UNH", "corp_HCA", "business_partner", 0.70, ev("public_record", "confirmed")),
        ("corp_ELV", "corp_HCA", "business_partner", 0.65, ev("public_record", "confirmed")),
        ("corp_CI", "corp_HCA", "business_partner", 0.60, ev("public_record", "confirmed")),
        ("corp_HUM", "corp_HCA", "business_partner", 0.55, ev("public_record", "confirmed")),
        # Health insurance competitors
        ("corp_UNH", "corp_ELV", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_UNH", "corp_CI", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_UNH", "corp_HUM", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_ELV", "corp_CI", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_ELV", "corp_HUM", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_CI", "corp_HUM", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_CNC", "corp_MOH", "competitor", 0.85, ev("industry", "confirmed")),
        # Consulting competitors
        ("corp_ACN", "corp_IBM", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_ACN", "corp_CAP", "competitor", 0.75, ev("industry", "confirmed")),
        # Data center / REIT → Tech
        ("corp_EQIX", "corp_MSFT", "business_partner", 0.70, ev("public_record", "confirmed")),
        ("corp_EQIX", "corp_AMZN", "business_partner", 0.65, ev("public_record", "confirmed")),
        ("corp_EQIX", "corp_GOOG", "business_partner", 0.65, ev("public_record", "confirmed")),
        # Index providers → Exchanges
        ("corp_SPGI", "corp_CME", "business_partner", 0.70, ev("public_record", "confirmed")),
        ("corp_MSCI", "corp_ICE", "business_partner", 0.60, ev("public_record", "confirmed")),
        # Agricultural chemicals
        ("corp_CF", "corp_MOS", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_DOW", "corp_DD", "competitor", 0.75, ev("industry", "confirmed")),
        ("corp_BAS", "corp_DOW", "competitor", 0.70, ev("industry", "confirmed")),
        # Building materials
        ("corp_VMC", "corp_MLM", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_SHW", "corp_PPG", "competitor", 0.85, ev("industry", "confirmed")),
        # Restaurant chains
        ("corp_MCD", "corp_SBUX", "competitor", 0.55, ev("industry", "confirmed")),
        ("corp_YUMC", "corp_MCD", "competitor", 0.50, ev("industry", "confirmed")),
        # Grocery
        ("corp_KR", "corp_WMT", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_KR", "corp_COST", "competitor", 0.65, ev("industry", "confirmed")),
        ("corp_WOW", "corp_COL2", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_TSCO", "corp_AD", "competitor", 0.65, ev("industry", "confirmed")),
        # Online food delivery
        ("corp_DASH", "corp_GRAB", "competitor", 0.40, ev("industry", "confirmed")),
        # Gold mining
        ("corp_ABX", "corp_AEM", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_ABX", "corp_NEM", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_NEM", "corp_AEM", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_ABX", "corp_AGL", "competitor", 0.70, ev("industry", "confirmed")),
        # Canadian energy
        ("corp_ENB", "corp_TRP", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_SU2", "corp_CNQ", "competitor", 0.80, ev("industry", "confirmed")),
        # Insurance brokers
        ("corp_AON", "corp_MMC", "competitor", 0.90, ev("industry", "confirmed")),
        # Latin American energy
        ("corp_PBR", "corp_ECOPETROL", "competitor", 0.60, ev("industry", "confirmed")),
        # Sports betting
        ("corp_DKNG", "corp_PENN", "competitor", 0.80, ev("industry", "confirmed")),
        # Travel booking
        ("corp_BKNG", "corp_EXPE", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_BKNG", "corp_ABNB", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_EXPE", "corp_ABNB", "competitor", 0.65, ev("industry", "confirmed")),
        ("corp_AMS", "corp_BKNG", "competitor", 0.40, ev("industry", "confirmed")),
        # Video conferencing
        ("corp_ZM", "corp_MSFT", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_ZM", "corp_CSCO", "competitor", 0.65, ev("industry", "confirmed")),
        # HR/Payroll tech
        ("corp_ADP", "corp_PAYX", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_ADP", "corp_WDAY", "competitor", 0.65, ev("industry", "confirmed")),
        ("corp_PAYC", "corp_PCTY", "competitor", 0.85, ev("industry", "confirmed")),
        # Document management
        ("corp_ADBE", "corp_DOCU", "competitor", 0.60, ev("industry", "confirmed")),
        # E-signature
        ("corp_DOCU", "corp_ADBE", "competitor", 0.65, ev("industry", "confirmed")),
        # Project management
        ("corp_TEAM", "corp_MNDY", "competitor", 0.70, ev("industry", "confirmed")),
        ("corp_TEAM", "corp_SMAR", "competitor", 0.65, ev("industry", "confirmed")),
        # Observability
        ("corp_DDOG", "corp_DT", "competitor", 0.75, ev("industry", "confirmed")),
        ("corp_DDOG", "corp_ESTC", "competitor", 0.70, ev("industry", "confirmed")),
        # Identity security
        ("corp_OKTA", "corp_CYBR", "competitor", 0.65, ev("industry", "confirmed")),
        ("corp_CRWD", "corp_S", "competitor", 0.75, ev("industry", "confirmed")),
        # DevOps
        ("corp_GTLB", "corp_MSFT", "competitor", 0.55, ev("industry", "confirmed")),
        # Solar
        ("corp_ENPH", "corp_SEDG", "competitor", 0.90, ev("industry", "confirmed")),
        ("corp_FSLR", "corp_ENPH", "competitor", 0.60, ev("industry", "confirmed")),
        # Nuclear/Clean energy
        ("corp_CEG", "corp_VST", "competitor", 0.70, ev("industry", "confirmed")),
        # Battery/EV supply
        ("corp_ALB", "corp_SQM", "competitor", 0.80, ev("industry", "confirmed")),
        ("corp_CATL", "corp_373220", "competitor", 0.90, ev("industry", "confirmed")),
        # Industrial automation
        ("corp_ROK", "corp_SIE", "competitor", 0.65, ev("industry", "confirmed")),
        ("corp_ROK", "corp_ABB", "competitor", 0.65, ev("industry", "confirmed")),
        ("corp_EMR", "corp_HON", "competitor", 0.70, ev("industry", "confirmed")),
        # HVAC
        ("corp_CARR", "corp_6367", "competitor", 0.65, ev("industry", "confirmed")),
        # Elevators
        ("corp_KONE", "corp_OTIS", "competitor", 0.85, ev("industry", "confirmed")),
        # Wire/Cable
        ("corp_ETN", "corp_HUBB", "competitor", 0.60, ev("industry", "confirmed")),
        # Buffett → Japanese trading houses
        ("corp_BRK", "corp_8001", "co_investor", 0.70, ev("sec_filing", "public_record")),
        ("corp_BRK", "corp_8058", "co_investor", 0.70, ev("sec_filing", "public_record")),
        ("corp_BRK", "corp_8031", "co_investor", 0.70, ev("sec_filing", "public_record")),
        # PE firm acquisitions/portfolio
        ("corp_BX", "corp_HLT", "co_investor", 0.60, ev("sec_filing", "public_record")),
        ("corp_KKR", "corp_USFD", "co_investor", 0.50, ev("sec_filing", "public_record")),
        # Stripe/Payments ecosystem
        ("corp_stripe", "corp_SHOP", "business_partner", 0.80, ev("public_record", "confirmed")),
        ("corp_stripe", "corp_AMZN", "business_partner", 0.60, ev("news_reports", "confirmed")),
        # Fintech → banking
        ("corp_HOOD", "corp_GS", "business_partner", 0.40, ev("news_reports", "estimated")),
        ("corp_SOFI", "corp_GS", "business_partner", 0.30, ev("news_reports", "estimated")),
        # Shipping competitors
        ("corp_MAERSK", "corp_cma_cgm", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_MAERSK", "corp_msc_shipping", "competitor", 0.85, ev("industry", "confirmed")),
        ("corp_cma_cgm", "corp_msc_shipping", "competitor", 0.85, ev("industry", "confirmed")),
    ]
    connections.extend(cross_sector)

    # ═══════════════════════════════════════════════
    # More leader appointments and same-party
    # ═══════════════════════════════════════════════
    more_political = [
        ("gov_us_trump", "bil_jared_isaacman", "appointed_by", 0.90, ev("news_reports", "confirmed")),  # NASA administrator
        ("gov_us_trump", "bil_musk", "appointed_by", 0.80, ev("news_reports", "confirmed")),  # DOGE
        ("gov_saudi_salman", "gov_saudi_mbs", "appointed_by", 0.99, ev("public_record", "confirmed")),
        ("gov_china_xi", "gov_china_li", "appointed_by", 0.95, ev("public_record", "confirmed")),
        ("gov_russia_putin", "gov_russia_mishustin", "appointed_by", 0.95, ev("public_record", "confirmed")),
        ("gov_uk_charles", "gov_uk_starmer", "appointed_by", 0.90, ev("public_record", "confirmed")),
        ("gov_japan_emperor", "gov_japan_ishiba", "appointed_by", 0.80, ev("public_record", "confirmed")),
        ("gov_spain_felipe", "gov_spain_sanchez", "appointed_by", 0.80, ev("public_record", "confirmed")),
        ("gov_netherlands_willem", "gov_netherlands_schoof", "appointed_by", 0.80, ev("public_record", "confirmed")),
        ("gov_belgium_philippe", "gov_belgium_de_croo", "appointed_by", 0.80, ev("public_record", "confirmed")),
        ("gov_sweden_carl_gustaf", "gov_sweden_kristersson", "appointed_by", 0.80, ev("public_record", "confirmed")),
        ("gov_norway_harald", "gov_norway_store", "appointed_by", 0.80, ev("public_record", "confirmed")),
        ("gov_denmark_frederik", "gov_denmark_frederiksen", "appointed_by", 0.80, ev("public_record", "confirmed")),
        ("gov_india_murmu", "gov_india_modi", "appointed_by", 0.85, ev("public_record", "confirmed")),
        ("gov_italy_mattarella", "gov_italy_meloni", "appointed_by", 0.80, ev("public_record", "confirmed")),
        ("gov_germany_steinmeier", "gov_germany_scholz", "appointed_by", 0.75, ev("public_record", "confirmed")),
        ("gov_france_macron", "gov_france_bayrou", "appointed_by", 0.90, ev("public_record", "confirmed")),
    ]
    connections.extend(more_political)

    # ═══════════════════════════════════════════════
    # PROGRAMMATIC: Generate bulk sector competitor pairs
    # For every pair of companies in the same sector/country combo,
    # add a competitor relationship if not already present
    # ═══════════════════════════════════════════════
    existing_pairs = set()
    for (a, b, rel, s, e) in connections:
        existing_pairs.add((a, b, rel))
        existing_pairs.add((b, a, rel))

    # Group companies by (sector, country) for same-market competitors
    from collections import defaultdict
    sector_groups = defaultdict(list)
    for (cid, name, ticker, sector, mcap, country, ceo) in get_companies():
        if mcap >= 10_000_000_000:  # Only $10B+ companies
            sector_groups[(sector, country)].append(cid)

    auto_competitor_count = 0
    for (sector, country), ids in sector_groups.items():
        if len(ids) < 2:
            continue
        # For each pair in the group (limit to avoid explosion)
        for i in range(len(ids)):
            for j in range(i + 1, min(i + 5, len(ids))):  # Max 4 pairs per company
                a, b = ids[i], ids[j]
                if (a, b, "competitor") not in existing_pairs:
                    connections.append((
                        a, b, "competitor", 0.50,
                        ev("industry_classification", "inferred")
                    ))
                    existing_pairs.add((a, b, "competitor"))
                    existing_pairs.add((b, a, "competitor"))
                    auto_competitor_count += 1

    log.debug(f"Auto-generated {auto_competitor_count} sector competitor pairs")

    # ═══════════════════════════════════════════════
    # PROGRAMMATIC: Generate billionaire → country leader
    # diplomatic_relationship for top billionaires in each country
    # ═══════════════════════════════════════════════
    # Map country → head of government
    country_leader = {}
    for (lid, name, title, country, party, tier) in get_world_leaders():
        if "Prime Minister" in title or "President" in title or "Chancellor" in title:
            # First one wins (head of government)
            if country not in country_leader:
                country_leader[country] = lid

    # Map billionaire country to leader
    auto_bil_gov = 0
    for (bid, name, nw, country, source, tier) in get_billionaires():
        if nw >= 20_000_000_000 and country in country_leader:
            lid = country_leader[country]
            if (bid, lid, "business_partner") not in existing_pairs:
                connections.append((
                    bid, lid, "business_partner", 0.30,
                    ev("inference", "inferred")
                ))
                existing_pairs.add((bid, lid, "business_partner"))
                auto_bil_gov += 1

    log.debug(f"Auto-generated {auto_bil_gov} billionaire-leader inferred connections")

    # ═══════════════════════════════════════════════
    # PROGRAMMATIC: Major institutional investors hold positions
    # in top companies — BlackRock, Vanguard, State Street
    # ═══════════════════════════════════════════════
    top_corps_by_mcap = sorted(
        get_companies(), key=lambda x: x[4], reverse=True
    )[:50]  # Top 50 by market cap

    for (cid, name, ticker, sector, mcap, country, ceo) in top_corps_by_mcap:
        if (cid, "corp_BLK", "co_investor") not in existing_pairs:
            connections.append((
                "corp_BLK", cid, "co_investor", 0.70,
                ev("sec_filing", "public_record")
            ))
            existing_pairs.add(("corp_BLK", cid, "co_investor"))

    # ═══════════════════════════════════════════════
    # PROGRAMMATIC: G20 leaders → IMF/World Bank/UN
    # ═══════════════════════════════════════════════
    g20_leaders = [
        "gov_us_trump", "gov_china_xi", "gov_india_modi", "gov_brazil_lula",
        "gov_russia_putin", "gov_japan_ishiba", "gov_germany_scholz",
        "gov_uk_starmer", "gov_france_macron", "gov_italy_meloni",
        "gov_canada_carney", "gov_south_korea_yoon", "gov_australia_albanese",
        "gov_mexico_sheinbaum", "gov_indonesia_prabowo", "gov_turkey_erdogan",
        "gov_saudi_mbs", "gov_argentina_milei", "gov_south_africa_ramaphosa",
    ]
    intl_orgs = ["gov_un_guterres", "gov_imf_georgieva", "gov_wb_banga"]
    for leader in g20_leaders:
        for org in intl_orgs:
            if (leader, org, "diplomatic_relationship") not in existing_pairs:
                connections.append((
                    leader, org, "diplomatic_relationship", 0.50,
                    ev("public_record", "confirmed")
                ))
                existing_pairs.add((leader, org, "diplomatic_relationship"))

    # ═══════════════════════════════════════════════
    # PROGRAMMATIC: All EU members → EU Commission
    # ═══════════════════════════════════════════════
    eu_leaders = [
        "gov_germany_scholz", "gov_france_macron", "gov_italy_meloni",
        "gov_spain_sanchez", "gov_netherlands_schoof", "gov_belgium_de_croo",
        "gov_austria_nehammer", "gov_poland_tusk", "gov_ireland_harris",
        "gov_portugal_montenegro", "gov_greece_mitsotakis", "gov_czech_fiala",
        "gov_romania_ciolacu", "gov_hungary_orban", "gov_sweden_kristersson",
        "gov_denmark_frederiksen", "gov_finland_orpo", "gov_croatia_plenkovic",
        "gov_bulgaria_glavchev", "gov_slovakia_fico", "gov_slovenia_golob",
        "gov_lithuania_simonyte", "gov_latvia_silina", "gov_estonia_michal",
        "gov_malta_abela", "gov_cyprus_christodoulides", "gov_luxembourg_frieden",
    ]
    for leader in eu_leaders:
        for target in ["gov_eu_vonderleyen", "gov_eu_costa", "ecb_lagarde"]:
            if (leader, target, "diplomatic_relationship") not in existing_pairs:
                connections.append((
                    leader, target, "diplomatic_relationship", 0.60,
                    ev("public_record", "confirmed")
                ))
                existing_pairs.add((leader, target, "diplomatic_relationship"))

    # ═══════════════════════════════════════════════
    # PROGRAMMATIC: All heads of state → head of gov (same country)
    # ═══════════════════════════════════════════════
    country_hos = {}  # head of state
    country_hog = {}  # head of government
    for (lid, name, title, country, party, tier) in get_world_leaders():
        if "King" in title or "Emperor" in title or "Sultan" in title or "Emir" in title:
            country_hos[country] = lid
        elif title == "Head of State" or (title == "President" and country not in country_hog):
            country_hos[country] = lid
        if "Prime Minister" in title or "Chancellor" in title or "Premier" in title:
            country_hog[country] = lid

    for country in set(country_hos.keys()) & set(country_hog.keys()):
        hos, hog = country_hos[country], country_hog[country]
        if hos != hog and (hos, hog, "appointed_by") not in existing_pairs:
            connections.append((
                hos, hog, "appointed_by", 0.80,
                ev("public_record", "confirmed")
            ))
            existing_pairs.add((hos, hog, "appointed_by"))

    return connections


# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════

def upsert_actor(conn, actor_id, name, category, tier, title=None, net_worth=None,
                 influence=None, trust=None, credibility="estimated", metadata=None):
    """Upsert a single actor."""
    if actor_id in EXISTING_IDS:
        log.debug(f"Skipping existing actor: {actor_id}")
        return False

    meta_json = json.dumps(metadata) if metadata else "{}"
    conn.execute(text("""
        INSERT INTO actors (id, name, category, tier, title, net_worth_estimate,
                            influence_score, trust_score, credibility, metadata, updated_at)
        VALUES (:id, :name, :category, :tier, :title, :net_worth,
                :influence, :trust, :credibility, CAST(:metadata AS jsonb), :updated_at)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            category = EXCLUDED.category,
            tier = EXCLUDED.tier,
            title = COALESCE(EXCLUDED.title, actors.title),
            net_worth_estimate = COALESCE(EXCLUDED.net_worth_estimate, actors.net_worth_estimate),
            influence_score = COALESCE(EXCLUDED.influence_score, actors.influence_score),
            trust_score = COALESCE(EXCLUDED.trust_score, actors.trust_score),
            credibility = EXCLUDED.credibility,
            metadata = CAST(:metadata AS jsonb),
            updated_at = EXCLUDED.updated_at
    """), {
        "id": actor_id,
        "name": name,
        "category": category,
        "tier": tier,
        "title": title,
        "net_worth": net_worth,
        "influence": influence,
        "trust": trust,
        "credibility": credibility,
        "metadata": meta_json,
        "updated_at": NOW,
    })
    return True


def insert_connection(conn, actor_a, actor_b, relationship, strength, evidence_json):
    """Insert a connection, skip on conflict."""
    conn.execute(text("""
        INSERT INTO actor_connections (actor_a, actor_b, relationship, strength, evidence, discovered_at)
        VALUES (:a, :b, :rel, :str, CAST(:ev AS jsonb), :ts)
        ON CONFLICT (actor_a, actor_b, relationship) DO NOTHING
    """), {
        "a": actor_a,
        "b": actor_b,
        "rel": relationship,
        "str": strength,
        "ev": evidence_json,
        "ts": NOW,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# INFLUENCE SCORING
# ═══════════════════════════════════════════════════════════════════════════════

def compute_influence(net_worth=None, tier=None, category=None):
    """Heuristic influence score 0–100."""
    score = 30  # baseline
    if net_worth:
        if net_worth >= 100_000_000_000:
            score += 40
        elif net_worth >= 50_000_000_000:
            score += 30
        elif net_worth >= 20_000_000_000:
            score += 20
        elif net_worth >= 5_000_000_000:
            score += 10
        else:
            score += 5
    if tier == "sovereign":
        score += 25
    elif tier == "regional":
        score += 15
    elif tier == "institutional":
        score += 5
    if category in ("government", "politician"):
        score += 10
    return min(score, 100)


def compute_trust(credibility):
    """Map credibility to a trust score."""
    mapping = {
        "confirmed": 90,
        "public_record": 80,
        "estimated": 60,
        "rumored": 30,
        "inferred": 40,
    }
    return mapping.get(credibility, 50)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    engine = get_engine()
    log.info("Starting VIP network seed...")

    billionaires = get_billionaires()
    leaders = get_world_leaders()
    companies = get_companies()
    connections = get_connections()

    log.info(f"Data loaded: {len(billionaires)} billionaires, {len(leaders)} leaders, "
             f"{len(companies)} companies, {len(connections)} connections")

    with engine.begin() as conn:
        # ── Seed billionaires ──
        bil_count = 0
        for (aid, name, nw, country, source, tier) in billionaires:
            if aid in EXISTING_IDS:
                continue
            influence = compute_influence(net_worth=nw, tier=tier, category="billionaire")
            trust = compute_trust("estimated")
            meta = {
                "country": country,
                "source_of_wealth": source,
                "net_worth_usd": nw,
                "data_source": "forbes_2025",
            }
            upsert_actor(conn, aid, name, "billionaire", tier,
                         title=f"Billionaire — {source}",
                         net_worth=nw,
                         influence=influence,
                         trust=trust,
                         credibility="estimated",
                         metadata=meta)
            bil_count += 1
        log.info(f"Seeded {bil_count} billionaires")

        # ── Seed world leaders ──
        gov_count = 0
        for (aid, name, title, country, party, tier) in leaders:
            if aid in EXISTING_IDS:
                continue
            category = "government"
            if "King" in title or "Emperor" in title or "Sultan" in title or "Emir" in title or "Monarchy" in party:
                category = "royal"
            elif "central" in title.lower() or "bank" in title.lower():
                category = "central_bank"
            influence = compute_influence(tier=tier, category=category)
            trust = compute_trust("public_record")
            meta = {
                "country": country,
                "political_party": party,
                "title": title,
                "data_source": "public_record_2025",
            }
            upsert_actor(conn, aid, name, category, tier,
                         title=title,
                         influence=influence,
                         trust=trust,
                         credibility="public_record",
                         metadata=meta)
            gov_count += 1
        log.info(f"Seeded {gov_count} world leaders")

        # ── Seed companies ──
        corp_count = 0
        for (aid, name, ticker, sector, mcap, country, ceo) in companies:
            if aid in EXISTING_IDS:
                continue
            tier = "sovereign" if mcap >= 500_000_000_000 else (
                "regional" if mcap >= 100_000_000_000 else "institutional"
            )
            influence = compute_influence(tier=tier, category="corporation")
            trust = compute_trust("confirmed")
            meta = {
                "ticker": ticker,
                "sector": sector,
                "market_cap_estimate": mcap,
                "country": country,
                "ceo": ceo,
                "data_source": "market_data_2025",
            }
            upsert_actor(conn, aid, name, "corporation", tier,
                         title=f"{sector} — {ticker}",
                         influence=influence,
                         trust=trust,
                         credibility="confirmed",
                         metadata=meta)
            corp_count += 1
        log.info(f"Seeded {corp_count} companies")

        # ── Seed connections ──
        conn_count = 0
        conn_skip = 0
        for (a, b, rel, strength, evidence) in connections:
            # Only insert if both actors exist (or are in EXISTING_IDS)
            # We'll let the DB handle FK constraints; if either doesn't exist, it'll fail silently
            try:
                insert_connection(conn, a, b, rel, strength, evidence)
                conn_count += 1
            except Exception as e:
                conn_skip += 1
                if conn_skip <= 10:
                    log.warning(f"Connection skip {a}→{b} ({rel}): {e}")

        log.info(f"Seeded {conn_count} connections ({conn_skip} skipped)")

    log.info("VIP network seed complete!")
    log.info(f"TOTALS: {bil_count} billionaires + {gov_count} leaders + "
             f"{corp_count} companies + {conn_count} connections")


if __name__ == "__main__":
    main()
