"""
S&P 500 Intelligence Profiles - Semiconductor & Tech Focus
Generated: 2026-03-28
Data basis: Public filings, proxy statements, 13F filings through early 2025
Confidence labels: [confirmed] = from SEC filings/proxy, [estimated] = from public reports,
                   [derived] = calculated from available data, [inferred] = pattern-based assessment
"""

sp500_intel_profiles = [
    {
        "ticker": "AVGO",
        "ceo": "Hock Tan [confirmed]",
        "ceo_comp": "$161.8M (FY2023 proxy, mostly stock awards) [confirmed]",
        "interlocks": [
            "Eddy Hartenstein - also on Lionsgate board [confirmed]",
            "Justine Page - former director at multiple tech cos [estimated]",
            "Harry You - also associated with GT Capital, prior Broadcom M&A advisor [confirmed]"
        ],
        "insider_pattern": "Net seller last 12 months. Hock Tan sold ~$100M+ in stock through 10b5-1 plans. Largest single transaction ~$48M (Dec 2024 area) [estimated]",
        "top_holders": [
            "Vanguard Group (~8.1%) [estimated]",
            "BlackRock (~7.5%) [estimated]",
            "Capital Research & Management (~5.2%) [estimated]"
        ],
        "tax_rate": "Effective ~11-13% vs 21% statutory. Significant IP held in Singapore where Broadcom has favorable tax arrangements. [derived]",
        "red_flags": [
            "CEO comp among highest in S&P 500 - $161.8M package drew shareholder scrutiny [confirmed]",
            "Serial acquirer model - VMware $69B acquisition integration risk [confirmed]",
            "Heavy reliance on AI/hyperscaler concentration - top 3 customers large revenue share [derived]"
        ]
    },
    {
        "ticker": "CRM",
        "ceo": "Marc Benioff [confirmed]",
        "ceo_comp": "$39.6M (FY2024 proxy) [confirmed]",
        "interlocks": [
            "Robin Washington - also on Alphabet/Google board [confirmed]",
            "Oscar Munoz - former United Airlines CEO, also on CBRE board [confirmed]",
            "Laura Alber - CEO of Williams-Sonoma, also on other boards [confirmed]"
        ],
        "insider_pattern": "Net seller. Benioff sold billions cumulatively through 10b5-1 plans. Largest recent transaction ~$50-100M blocks periodically [estimated]",
        "top_holders": [
            "Vanguard Group (~8.5%) [estimated]",
            "BlackRock (~7.2%) [estimated]",
            "State Street (~4.1%) [estimated]"
        ],
        "tax_rate": "Effective ~16-20% vs 21% statutory. R&D credits and international structuring keep rate below statutory. [derived]",
        "red_flags": [
            "Activist pressure from Elliott, Starboard, ValueAct in 2023 forced margin improvements [confirmed]",
            "Benioff persistent large-scale selling even as he pushes AI narrative [derived]",
            "Acquisition track record mixed - Slack integration still questioned on ROI [inferred]"
        ]
    },
    {
        "ticker": "NFLX",
        "ceo": "Ted Sarandos (co-CEO through 2024, sole CEO from Jan 2025 after Greg Peters became President/COO) [confirmed]",
        "ceo_comp": "$49.8M (2023 proxy, Sarandos) [confirmed]",
        "interlocks": [
            "Jay Hoag (TCV) - also on Zillow board [confirmed]",
            "Ann Mather - also on Alphabet/Google board [confirmed]",
            "Strive Masiyiwa - also on Unilever board [confirmed]"
        ],
        "insider_pattern": "Net seller. Reed Hastings sold substantial stakes post-departure. Sarandos regular seller through plans. Largest transactions $20-50M range [estimated]",
        "top_holders": [
            "Vanguard Group (~8.2%) [estimated]",
            "BlackRock (~6.9%) [estimated]",
            "Capital Research & Management (~4.5%) [estimated]"
        ],
        "tax_rate": "Effective ~14-17% vs 21% statutory. International content production and IP structuring through Netherlands/Ireland entities. [derived]",
        "red_flags": [
            "Content spend >$17B/yr creates massive cash burn vs accounting profit divergence [confirmed]",
            "Ad-tier growth may cannibalize premium tier margins [inferred]",
            "Password-sharing crackdown one-time boost now anniversarying [derived]"
        ]
    },
    {
        "ticker": "AMD",
        "ceo": "Lisa Su [confirmed]",
        "ceo_comp": "$30.3M (2023 proxy) [confirmed]",
        "interlocks": [
            "Nora Denzel - also on Ericsson board [confirmed]",
            "John Marren - TPG Capital connections, multiple board seats [estimated]",
            "Mark Durcan - former Micron CEO [confirmed]"
        ],
        "insider_pattern": "Net seller. Lisa Su sold ~$30-60M in 2024 through 10b5-1. Largest single block ~$25M [estimated]",
        "top_holders": [
            "Vanguard Group (~8.8%) [estimated]",
            "BlackRock (~7.6%) [estimated]",
            "State Street (~4.3%) [estimated]"
        ],
        "tax_rate": "Effective ~12-15% vs 21% statutory. Significant operations and IP in jurisdictions with tax holidays (Singapore, Ireland). [derived]",
        "red_flags": [
            "AI GPU market share vs NVIDIA still very small despite hype - MI300 adoption uncertain at scale [derived]",
            "Xilinx acquisition $49B still proving out synergies [confirmed]",
            "Lisa Su related to Jensen Huang (they are second cousins) - not a conflict but notable [confirmed]"
        ]
    },
    {
        "ticker": "TXN",
        "ceo": "Haviv Ilan (CEO from April 2023, succeeding Rich Templeton) [confirmed]",
        "ceo_comp": "$14.2M (2023 proxy, partial year as CEO) [estimated]",
        "interlocks": [
            "Mark Blinn - also on Flowserve board [estimated]",
            "Todd Williams - Lone Star Investment Advisors [estimated]",
            "Ronald Kirk - former US Trade Rep, multiple board connections [confirmed]"
        ],
        "insider_pattern": "Net seller. Moderate insider selling. Rich Templeton sold significant blocks on transition. Largest ~$15-25M [estimated]",
        "top_holders": [
            "Vanguard Group (~9.1%) [estimated]",
            "BlackRock (~7.8%) [estimated]",
            "State Street (~4.5%) [estimated]"
        ],
        "tax_rate": "Effective ~13-15% vs 21% statutory. Manufacturing incentives, CHIPS Act benefits, and Texas operations. [derived]",
        "red_flags": [
            "Massive $30B+ capex cycle for new 300mm fabs - FCF significantly depressed for years [confirmed]",
            "Analog/embedded cyclical downturn - inventory correction extended [confirmed]",
            "CEO transition risk - Haviv Ilan less proven than Templeton's decades of leadership [inferred]"
        ]
    },
    {
        "ticker": "QCOM",
        "ceo": "Cristiano Amon [confirmed]",
        "ceo_comp": "$27.6M (FY2023 proxy) [confirmed]",
        "interlocks": [
            "Mark McLaughlin - former Palo Alto Networks CEO, also on board of other tech cos [confirmed]",
            "Jamie Miller - former GE CFO [confirmed]",
            "Irene Rosenfeld - former Mondelez CEO, multiple S&P 500 board seats [confirmed]"
        ],
        "insider_pattern": "Net seller. Amon sold ~$20-40M through plans. CFO and other execs also regular sellers. [estimated]",
        "top_holders": [
            "Vanguard Group (~8.4%) [estimated]",
            "BlackRock (~7.1%) [estimated]",
            "State Street (~4.2%) [estimated]"
        ],
        "tax_rate": "Effective ~11-14% vs 21% statutory. Heavy offshore IP licensing structure, Singapore and other low-tax jurisdictions. [derived]",
        "red_flags": [
            "Apple modem chip insourcing risk - potential loss of largest customer [confirmed]",
            "Arm license dispute/renegotiation created ongoing legal uncertainty [confirmed]",
            "China revenue exposure >60% of QCT - geopolitical risk [confirmed]"
        ]
    },
    {
        "ticker": "INTU",
        "ceo": "Sasan Goodarzi [confirmed]",
        "ceo_comp": "$28.6M (FY2023 proxy) [confirmed]",
        "interlocks": [
            "Eve Burton - also on Discovery/Warner Bros Discovery connections [estimated]",
            "Suzanne Nora Johnson - former Goldman Sachs, also on Visa board [confirmed]",
            "Thomas Szkutak - former Amazon CFO [confirmed]"
        ],
        "insider_pattern": "Net seller. Goodarzi and other execs regular sellers. Largest transactions $10-20M range [estimated]",
        "top_holders": [
            "Vanguard Group (~8.3%) [estimated]",
            "BlackRock (~7.0%) [estimated]",
            "T. Rowe Price (~4.8%) [estimated]"
        ],
        "tax_rate": "Effective ~17-20% vs 21% statutory. Domestic-heavy revenue limits offshore tax optimization. [derived]",
        "red_flags": [
            "FTC/DOJ scrutiny over TurboTax Free File practices and deceptive advertising settlements [confirmed]",
            "Credit Karma acquisition - consumer credit data monetization regulatory risk [confirmed]",
            "AI disruption threat to core tax prep business model [inferred]"
        ]
    },
    {
        "ticker": "AMAT",
        "ceo": "Gary Dickerson [confirmed]",
        "ceo_comp": "$32.1M (FY2023 proxy) [confirmed]",
        "interlocks": [
            "Judy Bruner - also on Seagate and Verizon boards [confirmed]",
            "Xun (Eric) Chen - multiple tech company board connections [estimated]",
            "Scott Morgan - Bain Capital connections [estimated]"
        ],
        "insider_pattern": "Net seller. Dickerson sold ~$25-50M through plans in last 12 months. Regular scheduled selling. [estimated]",
        "top_holders": [
            "Vanguard Group (~8.6%) [estimated]",
            "BlackRock (~7.4%) [estimated]",
            "State Street (~4.3%) [estimated]"
        ],
        "tax_rate": "Effective ~12-14% vs 21% statutory. Significant international operations with IP in Singapore. [derived]",
        "red_flags": [
            "China export restrictions directly impact ~25-30% of revenue [confirmed]",
            "DOJ investigation into shipments to SMIC in China [confirmed]",
            "Cyclical WFE downturn risk after strong 2024 [derived]"
        ]
    },
    {
        "ticker": "MU",
        "ceo": "Sanjay Mehrotra [confirmed]",
        "ceo_comp": "$26.8M (FY2023 proxy) [confirmed]",
        "interlocks": [
            "Robert Switz - former ADC Telecom CEO, multiple board seats [confirmed]",
            "Lynn Dugle - also on other tech boards [estimated]",
            "Scott Decker - former Corning SVP [estimated]"
        ],
        "insider_pattern": "Net seller. Mehrotra sold ~$20-40M through 10b5-1 plans. Timing aligns with cyclical peaks. [estimated]",
        "top_holders": [
            "Vanguard Group (~8.9%) [estimated]",
            "BlackRock (~7.5%) [estimated]",
            "Capital Research & Management (~5.0%) [estimated]"
        ],
        "tax_rate": "Effective ~8-12% vs 21% statutory. Singapore manufacturing hub with significant tax holidays. [derived]",
        "red_flags": [
            "Extreme cyclicality - memory pricing swings cause massive earnings volatility [confirmed]",
            "China banned Micron from critical infrastructure (2023) - geopolitical retaliation risk [confirmed]",
            "HBM capacity ramp execution risk vs Samsung and SK Hynix [derived]"
        ]
    },
    {
        "ticker": "LRCX",
        "ceo": "Tim Archer [confirmed]",
        "ceo_comp": "$23.5M (FY2023 proxy) [estimated]",
        "interlocks": [
            "Abhijit Talwalkar - former LSI Logic CEO, multiple boards [confirmed]",
            "Catherine Lego - also on Coherent board [estimated]",
            "Eric Brandt - former Broadcom CFO, also on other tech boards [confirmed]"
        ],
        "insider_pattern": "Net seller. Archer and senior execs regular sellers. Moderate volumes ~$10-25M largest blocks. [estimated]",
        "top_holders": [
            "Vanguard Group (~8.7%) [estimated]",
            "BlackRock (~7.3%) [estimated]",
            "State Street (~4.4%) [estimated]"
        ],
        "tax_rate": "Effective ~11-13% vs 21% statutory. Significant international operations, IP in low-tax jurisdictions. [derived]",
        "red_flags": [
            "China revenue exposure ~25-30%, directly hit by export controls [confirmed]",
            "WFE cyclicality - capex-dependent revenue model [confirmed]",
            "Customer concentration - TSMC, Samsung, Intel top 3 are huge share of revenue [derived]"
        ]
    },
    {
        "ticker": "KLAC",
        "ceo": "Rick Wallace [confirmed]",
        "ceo_comp": "$19.8M (FY2023 proxy) [estimated]",
        "interlocks": [
            "Marie Myers - HP Inc CFO, also on other boards [estimated]",
            "Robert Calderoni - also on Citrix/other tech boards [estimated]",
            "Kiran Patel - multiple semiconductor industry board connections [estimated]"
        ],
        "insider_pattern": "Net seller. Wallace consistent seller through plans. ~$15-30M in largest annual sales. [estimated]",
        "top_holders": [
            "Vanguard Group (~9.2%) [estimated]",
            "BlackRock (~7.8%) [estimated]",
            "Capital Research & Management (~5.5%) [estimated]"
        ],
        "tax_rate": "Effective ~12-14% vs 21% statutory. International IP structure and R&D credits. [derived]",
        "red_flags": [
            "China exposure ~25%+ of revenue, export control headwinds [confirmed]",
            "Process control niche - if EUV adoption changes inspection needs [inferred]",
            "Aggressive capital return via buybacks funded partly by debt [derived]"
        ]
    },
    {
        "ticker": "SNPS",
        "ceo": "Sassine Ghazi (CEO from Jan 2024, succeeding Aart de Geus who became Executive Chair) [confirmed]",
        "ceo_comp": "$15.2M (FY2023 proxy, partial as CEO) [estimated]",
        "interlocks": [
            "Aart de Geus (Exec Chair) - massive industry connections across EDA/semiconductor [confirmed]",
            "Mercedes Johnson - also on Teradyne and other boards [confirmed]",
            "John Schwarz - former Visage/Business Objects CEO [estimated]"
        ],
        "insider_pattern": "Net seller. Aart de Geus sold large blocks through 2024 on transition. Ghazi beginning regular selling. ~$30M+ for de Geus. [estimated]",
        "top_holders": [
            "Vanguard Group (~8.5%) [estimated]",
            "BlackRock (~7.2%) [estimated]",
            "T. Rowe Price (~5.0%) [estimated]"
        ],
        "tax_rate": "Effective ~16-19% vs 21% statutory. Domestic-heavy with some international optimization. R&D credit benefit. [derived]",
        "red_flags": [
            "Ansys acquisition ($35B) - massive integration risk, DOJ antitrust review [confirmed]",
            "CEO transition from legendary founder - execution risk [inferred]",
            "EDA duopoly with CDNS could attract antitrust scrutiny [derived]"
        ]
    },
    {
        "ticker": "CDNS",
        "ceo": "Anirudh Devgan [confirmed]",
        "ceo_comp": "$17.8M (FY2023 proxy) [estimated]",
        "interlocks": [
            "John Wall - also on Qorvo board [estimated]",
            "Mary Louise Krakauer - multiple tech board connections [estimated]",
            "Alberto Sangiovanni-Vincentelli - UC Berkeley, deep academic/industry network [confirmed]"
        ],
        "insider_pattern": "Net seller. Devgan regular seller ~$15-25M. Lip-Bu Tan (former CEO) sold significant blocks before departure. [estimated]",
        "top_holders": [
            "Vanguard Group (~8.8%) [estimated]",
            "BlackRock (~7.4%) [estimated]",
            "T. Rowe Price (~4.9%) [estimated]"
        ],
        "tax_rate": "Effective ~15-18% vs 21% statutory. R&D credits and some international IP structuring. [derived]",
        "red_flags": [
            "EDA duopoly with SNPS - regulatory risk if scrutiny increases [derived]",
            "AI-native chip design tools could disrupt traditional EDA if open-source alternatives emerge [inferred]",
            "Lip-Bu Tan departure to Intel - lost a legendary operator [confirmed]"
        ]
    },
    {
        "ticker": "ADI",
        "ceo": "Vincent Roche [confirmed]",
        "ceo_comp": "$18.5M (FY2023 proxy) [estimated]",
        "interlocks": [
            "Karen Golz - former EY Global Vice Chair, also on other boards [confirmed]",
            "Edward Frank - also on other tech boards [estimated]",
            "Kenton Sicchitano - multiple board seats in industrials [estimated]"
        ],
        "insider_pattern": "Net seller. Roche moderate seller ~$10-20M. Insider selling increased near all-time highs. [estimated]",
        "top_holders": [
            "Vanguard Group (~8.6%) [estimated]",
            "BlackRock (~7.5%) [estimated]",
            "State Street (~4.5%) [estimated]"
        ],
        "tax_rate": "Effective ~11-14% vs 21% statutory. Ireland HQ (Maxim legacy), significant international IP. [derived]",
        "red_flags": [
            "Maxim Integrated acquisition integration - still optimizing combined portfolio [confirmed]",
            "Analog/industrial downcycle extended into 2024 - inventory correction [confirmed]",
            "Automotive exposure risk if EV adoption slows or China competition intensifies [inferred]"
        ]
    },
    {
        "ticker": "MRVL",
        "ceo": "Matt Murphy [confirmed]",
        "ceo_comp": "$26.4M (FY2024 proxy) [estimated]",
        "interlocks": [
            "Oleg Khaykin - CEO of Viavi Solutions [confirmed]",
            "Peter Feld - Starboard Value connections, activist background [confirmed]",
            "Tudor Brown - former ARM president, multiple semiconductor boards [confirmed]"
        ],
        "insider_pattern": "Net seller. Murphy sold ~$20-40M through plans as stock recovered. Largest transactions ~$15M blocks. [estimated]",
        "top_holders": [
            "Vanguard Group (~8.3%) [estimated]",
            "BlackRock (~7.1%) [estimated]",
            "Capital Research & Management (~5.5%) [estimated]"
        ],
        "tax_rate": "Effective ~5-9% vs 21% statutory. Bermuda-incorporated, significant tax advantages from offshore structure. [derived]",
        "red_flags": [
            "Very low effective tax rate - Bermuda incorporation could face reform pressure [derived]",
            "Custom silicon for hyperscalers is customer-concentrated and design-win dependent [confirmed]",
            "Inphi, Innovium acquisitions still proving revenue synergies [confirmed]"
        ]
    },
    {
        "ticker": "ON",
        "ceo": "Hassane El-Khoury [confirmed]",
        "ceo_comp": "$17.2M (2023 proxy) [estimated]",
        "interlocks": [
            "Atsushi Horiba - HORIBA Ltd chairman, cross-industry connections [confirmed]",
            "Susan Faulkner - multiple board seats [estimated]",
            "Gregory Waters - semiconductor industry veteran [estimated]"
        ],
        "insider_pattern": "Net seller. El-Khoury sold ~$15-30M through plans during 2024. Other execs also selling. [estimated]",
        "top_holders": [
            "Vanguard Group (~9.0%) [estimated]",
            "BlackRock (~7.6%) [estimated]",
            "State Street (~4.5%) [estimated]"
        ],
        "tax_rate": "Effective ~14-17% vs 21% statutory. International manufacturing footprint helps. [derived]",
        "red_flags": [
            "EV/automotive slowdown directly impacts SiC power semiconductor demand [confirmed]",
            "Inventory correction in industrial/automotive extended through 2024 [confirmed]",
            "SiC capacity buildout vs weakening demand - capex timing risk [derived]"
        ]
    },
    {
        "ticker": "GFS",
        "ceo": "Thomas Caulfield [confirmed]",
        "ceo_comp": "$14.5M (2023 proxy) [estimated]",
        "interlocks": [
            "Board heavily influenced by Mubadala (Abu Dhabi sovereign fund) which owns ~80% [confirmed]",
            "Khaldoon Al Mubarak - Mubadala CEO, also Man City chairman, Abu Dhabi power broker [confirmed]",
            "Amir Faintuch - semiconductor industry connections [estimated]"
        ],
        "insider_pattern": "Limited public float trading. Mubadala controls ~80%. Insider selling minimal due to concentrated ownership. [confirmed]",
        "top_holders": [
            "Mubadala Investment Company (~80%) [confirmed]",
            "Vanguard Group (~2.5% of float) [estimated]",
            "BlackRock (~2.0% of float) [estimated]"
        ],
        "tax_rate": "Effective ~5-10% vs 21% statutory. Singapore and Germany operations with significant incentives, plus Malta structure. [derived]",
        "red_flags": [
            "Mubadala supermajority control - minority shareholders have minimal influence [confirmed]",
            "Trailing-edge foundry facing competition from SMIC and other Chinese fabs [confirmed]",
            "No EUV capability - limited ability to compete for advanced nodes [confirmed]"
        ]
    },
    {
        "ticker": "ARM",
        "ceo": "Rene Haas [confirmed]",
        "ceo_comp": "$16.9M (FY2024 proxy) [estimated]",
        "interlocks": [
            "SoftBank controls ~90% through subsidiary - Masayoshi Son is key decision maker [confirmed]",
            "Carolyn Herzog - former Zscaler CLO [estimated]",
            "Young Sohn - former Samsung Strategy head, venture connections [confirmed]"
        ],
        "insider_pattern": "Very limited public float. SoftBank owns ~90%. Haas minimal selling. IPO lockup expirations drove some flows. [confirmed]",
        "top_holders": [
            "SoftBank Group (~90%) [confirmed]",
            "Vanguard Group (~1.5% of float) [estimated]",
            "BlackRock (~1.2% of float) [estimated]"
        ],
        "tax_rate": "Effective ~15-18% vs 21% statutory. UK-headquartered, benefits from UK patent box regime. [derived]",
        "red_flags": [
            "SoftBank ~90% ownership - true public float extremely small, valuation distorted [confirmed]",
            "Qualcomm license dispute over Nuvia architecture - existential licensing model risk [confirmed]",
            "RISC-V open-source alternative gaining traction in China and edge markets [confirmed]",
            "Valuation at IPO implied >$60B for a $3B revenue licensing company [confirmed]"
        ]
    },
    {
        "ticker": "ASML",
        "ceo": "Christophe Fouquet (CEO from April 2024, succeeding Peter Wennink) [confirmed]",
        "ceo_comp": "$7.5M EUR (~$8.2M USD, 2023 proxy, Wennink's final year) [estimated]",
        "interlocks": [
            "Nils Andersen - also on Unilever and other European boards [confirmed]",
            "Gerard Kleisterlee - former Philips CEO, also on Vodafone board [confirmed]",
            "Lena Olving - multiple European industrial boards [estimated]"
        ],
        "insider_pattern": "Net seller. Wennink sold significant blocks through 2024 on departure. Fouquet beginning smaller sales. European disclosure rules differ. [estimated]",
        "top_holders": [
            "Capital Research & Management (~14%) [estimated]",
            "BlackRock (~7.5%) [estimated]",
            "Baillie Gifford (~4.0%) [estimated]"
        ],
        "tax_rate": "Effective ~14-16% vs Netherlands statutory 25.8%. Innovation box regime reduces effective rate significantly. [derived]",
        "red_flags": [
            "Monopoly on EUV lithography - geopolitical weapon, Netherlands/US export controls to China [confirmed]",
            "China revenue was ~30%+ in 2023-2024, now severely restricted [confirmed]",
            "CEO transition from iconic Wennink to Fouquet - execution risk [inferred]",
            "High-NA EUV ($350M+/tool) adoption pace uncertain [confirmed]"
        ]
    },
    {
        "ticker": "TSM",
        "ceo": "C.C. Wei (CEO) [confirmed]",
        "ceo_comp": "$~6-8M USD equivalent (Taiwanese companies disclose differently, much lower than US peers) [estimated]",
        "interlocks": [
            "Mark Liu - TSMC Chairman, massive government/industry influence in Taiwan [confirmed]",
            "Board is heavily Taiwan-centric with government connections [confirmed]",
            "Sir Peter Bonfield - international director, also on other global boards [estimated]"
        ],
        "insider_pattern": "Minimal insider selling in traditional sense. Taiwanese insider trading rules differ. Mark Liu and C.C. Wei hold relatively modest stakes. ADR structure complicates tracking. [derived]",
        "top_holders": [
            "National Development Fund (Taiwan government) (~6.4%) [confirmed]",
            "Vanguard Group (~2.5% via ADR) [estimated]",
            "BlackRock (~2.0% via ADR) [estimated]"
        ],
        "tax_rate": "Effective ~14-16% vs Taiwan statutory 20%. Tax incentives for semiconductor investment. US fab costs will increase blended rate over time. [derived]",
        "red_flags": [
            "Taiwan Strait geopolitical risk - existential single point of failure for global semiconductor supply [confirmed]",
            "Arizona fab delays and cost overruns ($40B+ investment) [confirmed]",
            "Customer concentration: Apple ~25%, NVIDIA, AMD, Qualcomm are massive share [confirmed]",
            "N2/A16 technology leadership must be maintained vs Samsung and Intel pressure [derived]"
        ]
    }
]
