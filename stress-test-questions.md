Yes. A good way to pressure-test Energy Atlas is to build question sets that target:

1. **keyword collisions**
2. **ambiguous geography**
3. **mixed metrics in one sentence**
4. **time-range edge cases**
5. **synonyms your router does not know**
6. **default fallbacks that may silently misroute**
7. **queries that sound natural but bypass exact keywords**

Below is a structured list by category, designed specifically against your current router.

---

# 1. Storage

These test `working_gas_storage_lower48` and `working_gas_storage_change_weekly`, plus region routing.

## Straightforward but useful

* What is Lower 48 working gas storage right now? ✅
* Show weekly storage change for the East region over the last 12 weeks. ✅
* How much gas was injected into storage in the Midwest this month? ✅
* Did the South Central region post a withdrawal last week? ✅
* Compare Pacific and Mountain storage inventories this winter? ✅


## Edge cases

* What happened to inventories last week in the eastern U.S.?

  * “eastern U.S.” may or may not match `"eastern"` cleanly depending on `contains_any`.
* Show me storage for the western region.  ✅

  * You do not have a `"west"` or `"western"` mapping; likely falls back to lower48.
* How much gas is in storage in the South? ❌

  * “South” is ambiguous and may fail to map to `south_central`.
* Give me the weekly injection number for the lower forty-eight. ✅

  * “lower forty-eight” will probably miss `lower48` and `lower 48`.
* Was there a draw in inventories last Thursday? ✅

  * “draw” is common market language but not explicitly in keywords.
* Show storage build by region. ❌

  * “build” is not in the change keywords.
* How tight is storage versus normal? ✅

  * Could fail entirely; no “vs 5-year average” concept.
* Where are withdrawals happening fastest? ❌

  * Multi-region comparative intent not represented in router.
* Compare East storage and weekly change together. ❌

  * Router only returns one metric.
* Show me storage for Appalachia. ✅

  * No regional alias for EIA storage regions.

## Keyword collision tests

* Show gas demand and storage in ERCOT. ❌

  * “storage” routes EIA storage; ERCOT is ignored because ISO filters only attach for `iso_` metrics.
* Show power demand and storage in Texas. ✅

---

# 2. Henry Hub / Price

These test `henry_hub_spot`.

## Straightforward

* What is the Henry Hub spot price today?
* Show natural gas benchmark price over the last 30 days.
* How has Henry Hub changed since January?
* Plot spot gas prices over the past 12 months.
* What was the average benchmark gas price last winter?

## Edge cases

* What’s the front-month gas price?

  * Futures concept, likely not routeable.
* Show me natural gas prices.

  * Could route via `"gas price"` if present, but broad phrase is fragile.
* What did gas trade at last Friday?

  * “trade at” may miss.
* Compare cash gas versus Henry Hub.

  * “cash gas” is not recognized.
* What is prompt month nat gas doing?

  * Futures slang not recognized.
* Show spot prices excluding weekends.

  * Query parser likely can’t operationalize this.
* What was gas under $3?

  * Intent is threshold/event detection, not simple series retrieval.
* When did Henry Hub last spike above 5?

  * Event query, not just chart/data pull.
* Show price trend since the Freeport outage.

  * Relative event anchor likely unsupported.
* Gas prices in Texas this week.

  * Might incorrectly route Henry Hub due to “gas price,” though user may mean regional power/gas market pricing.

## Collision tests

* Show gas price and LNG exports together.

  * Router can only choose one metric.
* Is gas price up because of storage withdrawals?

  * Causal question with two metrics.

---

# 3. LNG Exports / Imports / Trade Flows

These test `lng_exports`, `lng_imports`, and trade region routing.

## Straightforward

* Show LNG exports over the last year.
* What are LNG imports doing this month?
* Compare LNG imports versus LNG exports over time.
* Show U.S. total pipeline exports.
* How much gas is flowing to Mexico?

## Edge cases

* Show exports to Mexico.

  * Your region router only has `mexico_pipeline`, but metric detection may choose `lng_exports` from “exports” even though user really means pipeline exports.
* Show Canadian imports versus Mexican exports.

  * One query implies two regions and maybe two directions.
* Is the U.S. a net exporter right now?

  * Requires combining imports and exports; router returns one metric.
* Which regions import vs export the most?

  * Keyword exists under `lng_exports`, but answer likely needs multiple metrics/regions.
* Show cross-border pipeline flow.

  * Likely hits `lng_exports`, but conceptually broader than LNG.
* Show gas flow to Canada.

  * Could route `lng_exports`; unclear directional semantics.
* Show LNG utilization.

  * May route by `"lng capacity utilization"` if present, but output metric may not actually represent that.
* What is liquefaction utilization this week?

  * “liquefaction” is not in keywords.
* Show feedgas and exports.

  * Feedgas not in router.
* Compare pipeline exports and LNG exports.

  * One metric cannot represent both.
* Imports versus exports by border.

  * Multi-dimensional comparative query.
* Show import/export balance.

  * You have some keywords for imports vs exports, but metric result is still only `lng_exports`.

## Region edge cases

* Show U.S. exports.

  * No explicit mapping for generic “U.S. exports” unless default total.
* Show total exports.

  * Might default silently to `united_states_pipeline_total`.
* Show exports south of the border.

  * Won’t map to Mexico.
* Show gas imports from the north.

  * Won’t map to Canada.
* Show pipeline trade with Canada and Mexico together.

  * Two regions in one question.

---

# 4. Consumption

These test `ng_consumption_lower48`.

## Straightforward

* Show U.S. natural gas consumption over the last year.
* Which month had the highest gas usage last year?
* How much gas do consumers use in winter?
* Show consumption trend since 2020.
* Plot Lower 48 gas usage by month.

## Edge cases

* Show demand for natural gas.

  * You intentionally excluded “demand,” so likely fails or routes to `iso_load`.
* How much gas are we burning?

  * Not clearly covered.
* Show end-use gas demand.

  * “demand” ambiguity again.
* How much gas is being used across the economy?

  * Natural phrasing but may miss keywords.
* Show residential/commercial/industrial consumption.

  * If dataset supports sectors, router does not.
* Who consumes the most gas?

  * Entity breakdown intent, not series retrieval.
* Compare consumption versus electricity burn.

  * Two metrics requested.
* Show total gas use excluding power plants.

  * Requires sector filtering.
* Show weather-adjusted gas use.

  * Advanced derived query, not directly routed.
* Show gas usage during the cold snap.

  * Event-relative.

## Collision tests

* Show demand in Texas.

  * Could mean `iso_load`, `ng_consumption_lower48`, or even ERCOT load.
* Show gas demand in ERCOT.

  * “gas demand” may not match consumption and could instead hit `iso_load` if “demand” dominates.

---

# 5. Electricity / Gas-fired Power

These test `ng_electricity`.

## Straightforward

* How much gas is used for electricity generation?
* Show natural gas consumed by power plants.
* Plot gas burn for electricity over time.
* Compare gas use in electricity this summer versus last summer.
* How much gas did power generators use last month?

## Edge cases

* Show gas burn.

  * Very common term, but not explicitly in `ng_electricity`; it exists under `iso_gas_dependency` instead.
* How much gas did generators burn in the Lower 48?

  * Might collide between EIA electricity and ISO gas dependency.
* Show power burn this week.

  * “power burn” not explicitly captured.
* Compare gas-fired generation and gas for electricity.

  * Could route to ISO fuel mix or EIA electricity depending on phrase.
* Show electricity sector gas demand.

  * “demand” ambiguity.
* How much fuel did gas plants consume?

  * Natural but not directly keyed.
* Show gas use in power generation versus industrial use.

  * Multi-metric.
* Show gas burn in PJM.

  * Could incorrectly go ISO rather than EIA electricity.
* Show electric-sector gas use in Texas.

  * Texas suggests ERCOT, but electric-sector gas use sounds EIA.
* Show gas-fired output.

  * Could imply generation, not fuel consumption.

---

# 6. Production

These test `ng_production_lower48`.

## Straightforward

* Show dry gas production over the last year.
* How much natural gas is being produced in the Lower 48?
* Plot production growth since 2022.
* Was production higher this month than last month?
* Show output trend for U.S. gas supply.

## Edge cases

* Show gas supply.

  * You included “supply,” but this can also semantically mean storage or total availability.
* How much gas came out of the ground last week?

  * Natural phrasing likely misses.
* Show marketed production.

  * You only key to dry gas production.
* Show shale gas output.

  * Unsupported subtype.
* Show Appalachia production.

  * No basin routing.
* What is production doing in Texas?

  * State filter unsupported.
* Compare output and demand.

  * Production vs load/consumption collision.
* How tight is supply?

  * Not equal to production; analytical question.
* Show upstream gas output.

  * “upstream” not mapped.
* Was there a production freeze-off impact?

  * Event-relative, derived.

---

# 7. Exploration / Reserves

These test `ng_exploration_reserves_lower48`.

## Straightforward

* Show proved reserves over time.
* How have U.S. gas reserves changed?
* Plot exploration and reserves trends.
* What are the latest proved natural gas reserves?
* Did reserves grow year over year?

## Edge cases

* Show reserves by basin.

  * Unsupported geography.
* Show drilling activity.

  * User intent may be exploration, but keyword route may fail.
* How much gas is still in the ground?

  * Natural phrasing may miss.
* Show discoveries versus reserves.

  * Not the same metric.
* Show reserves replacement.

  * Derived upstream metric.
* How active is exploration?

  * Could fail.
* Show gas resource base.

  * Different term than proved reserves.
* Compare reserves and production.

  * Two metrics.
* How long would reserves last at current production?

  * Derived ratio question.
* Which states have the most reserves?

  * State granularity unsupported.

---

# 8. ISO Load

These test `iso_load`.

## Straightforward

* Show ERCOT load today.
* Plot PJM demand over the last 7 days.
* How high was NYISO system demand yesterday?
* Show CAISO electric demand this week.
* Compare ISO-NE load this winter versus last winter.

## Edge cases

* Show Texas demand.

  * Good test of ISO aliasing to ERCOT.
* Show New York demand.

  * Could route NYISO, but “New York” is also a state reference elsewhere.
* Show New England power usage.

  * “power usage” may or may not map.
* Show electricity demand in California.

  * Should hit CAISO, but good natural-language test.
* Show peak load.

  * No explicit “peak” semantics.
* When was system demand highest this month?

  * Event query.
* Show load net of renewables.

  * Derived metric unsupported.
* Show load during the heat wave.

  * Event-relative.
* Compare load in PJM and ERCOT.

  * Multi-ISO unsupported.
* Show demand for the Northeast.

  * Could ambiguously imply ISO-NE or PJM or NYISO.

## Geography failure tests

* Show load in Boston.

  * No city alias to ISO-NE.
* Show load in DC.

  * DC is in PJM keyword list, useful test.
* Show load in Pennsylvania and New York.

  * Two ISOs potentially implied.

---

# 9. ISO Gas Dependency

These test `iso_gas_dependency`.

## Straightforward

* How much gas generation is ERCOT using?
* Show gas share in PJM.
* Plot gas dependency for ISO-NE.
* What percent of NYISO generation came from gas?
* Show gas-fired generation in CAISO.

## Edge cases

* Show gas burn in ERCOT.

  * Common term, useful.
* How dependent is Texas on gas for power?

  * Natural phrasing.
* Show fuel switching away from gas.

  * Not the same metric.
* How much gas-fired power was dispatched yesterday?

  * Dispatch wording present, but semantics may vary.
* Show gas generation share during the cold snap.

  * Event-relative.
* Compare gas dependency in ERCOT and PJM.

  * Multi-ISO unsupported.
* Show gas share excluding dual-fuel plants.

  * Unsupported filtering.
* How much gas was burned for electricity in Texas?

  * Could collide with `ng_electricity`.
* Show natural gas share of the grid.

  * Good natural phrasing.
* Did gas dominate the stack this morning?

  * Intraday-ish phrasing and slang.

---

# 10. ISO Renewables

These test `iso_renewables`.

## Straightforward

* Show renewables in ERCOT.
* Plot renewable generation in CAISO.
* What share of PJM generation came from renewables?
* Compare NYISO wind and solar over the last month.
* Show renewable output in ISO-NE.

## Edge cases

* Show clean energy generation in Texas.

  * “clean energy” not in keywords.
* Show green power share in California.

  * Not in keywords.
* Plot wind plus solar for ERCOT.

  * Likely works; good test.
* Show solar only in CAISO.

  * Router does not distinguish subtypes.
* Show wind ramp in ERCOT.

  * “ramp” unsupported.
* Compare renewables and gas share in PJM.

  * Two metrics.
* Show intermittent generation in New England.

  * Not in keywords.
* Show renewable penetration.

  * Common industry phrase not in keywords.
* How much of the grid was renewable yesterday?

  * Natural phrasing.
* Show renewables excluding hydro.

  * Unsupported filtering.

---

# 11. ISO Fuel Mix

These test `iso_fuel_mix`.

## Straightforward

* Show ERCOT fuel mix.
* Plot CAISO generation mix over time.
* What was NYISO power mix yesterday?
* Show PJM generation by fuel.
* Compare ISO-NE fuel mix this winter versus summer.

## Edge cases

* What was on the stack in ERCOT?

  * “stack” not in keywords.
* Show generation by source in California.

  * “source” not in keywords.
* Show the grid mix in Texas.

  * “grid mix” not in keywords.
* Compare gas, coal, and nuclear in PJM.

  * Detailed component request.
* Show fuel mix excluding imports.

  * Unsupported filtering.
* Show hourly mix.

  * Temporal granularity may be unsupported by downstream data.
* Show power composition in New York.

  * “composition” not in keywords.
* Show the dispatch stack by fuel.

  * Might hit due to dispatch keyword in gas dependency, not fuel mix.
* What fuels kept the lights on last night in ISO-NE?

  * Natural phrasing.
* Compare fuel mix and load together.

  * Two metrics.

---

# 12. Cross-category ambiguity tests

These are some of the best router stress tests.

## Same word, different meanings

* Show gas demand in Texas.

  * Could mean gas consumption, electric load, or gas-fired generation.
* Show power demand and gas demand in ERCOT.

  * Two concepts with same keyword family.
* How much gas is ERCOT using?

  * Could mean gas share of generation, gas-fired output, or gas consumption by power plants.
* Show California generation and consumption.

  * Power generation vs gas consumption.
* Show storage and supply.

  * Could mean storage + production.

## Multi-metric questions

* Compare Henry Hub price, storage, and LNG exports over the last year.
* How did price react to storage withdrawals?
* Show production, consumption, and exports on one chart.
* Did ERCOT gas dependency rise when Henry Hub spiked?
* Compare renewables share and load in CAISO during the heat wave.

## Cross-domain geo ambiguity

* Show New York gas demand.

  * NYISO load? state gas consumption? city load?
* Show Texas gas generation and storage.

  * ERCOT + EIA storage.
* Show California gas prices and renewable generation.

  * Henry Hub + CAISO.
* Show New England fuel mix and gas storage.

  * ISO-NE + EIA storage region mismatch.

---

# 13. Time parsing stress tests

These are important because router output depends on `resolve_date_range(user_query)`.

## Natural phrasing

* today
* yesterday
* last week
* this winter
* last winter
* since January
* year to date
* month to date
* over the past 90 days
* over the last twelve months

## Edge cases

* from Thanksgiving to Christmas
* since the February freeze
* around the Freeport outage
* over the shoulder season
* during the last cold snap
* from Jan 2024 through Easter
* this heating season
* last injection season
* last withdrawal season
* the week before last

These are good because even if metric routing succeeds, date parsing may fail or produce surprising boundaries.

---

# 14. Queries your current router will likely miss

These are especially valuable because they represent realistic user phrasing.

## Storage

* How full are inventories?
* Was there a storage build?
* Was there a draw?
* How tight is storage?

## Price

* What’s nat gas doing?
* Show cash gas.
* Show gas cash price.

## Production

* How much gas are we producing?
* Show upstream output.
* Show field production.

## Consumption / electricity

* Show gas burn.
* Show electric-sector burn.
* How much gas are generators using?

## Trade

* Show cross-border flows.
* Show sendout to Mexico.
* Show imports from Canada.

## ISO

* Show renewable penetration.
* Show dispatch stack.
* Show net load.
* Show clean energy share.

---

# 15. Best edge-case question bank by category

If your goal is to build a formal test suite, this is the short list I would start with.

## Storage

* Was there a draw in the lower forty-eight last week?
* Show storage build in the South.
* Compare East storage and weekly change.
* How tight is storage versus normal?
* Show western region inventories.

## Price

* What’s nat gas doing this week?
* When did Henry Hub last spike above 5?
* Show gas prices since the Freeport outage.
* Compare Henry Hub and storage.
* What did gas trade at last Friday?

## LNG / trade

* Is the U.S. a net exporter right now?
* Show exports to Mexico.
* Show imports from Canada.
* Compare pipeline exports and LNG exports.
* Which regions import versus export the most?

## Consumption

* Show gas demand.
* How much gas are we burning across the economy?
* Compare consumption versus electricity burn.
* Show residential and industrial gas usage.
* Show weather-adjusted gas use.

## Electricity

* Show gas burn for power plants.
* How much gas did generators burn in Texas?
* Compare gas-fired generation and electric-sector gas use.
* Show power burn this week.
* Show gas use in power generation versus industrial use.

## Production

* How much gas came out of the ground last week?
* Show marketed production.
* What is production doing in Texas?
* Compare output and demand.
* Was there a freeze-off impact?

## Reserves

* How much gas is still in the ground?
* Show drilling activity and reserves.
* How long would reserves last at current production?
* Which states have the most reserves?
* Show reserves replacement.

## ISO load

* Show Texas demand.
* Show New England power usage.
* When was system demand highest this month?
* Compare load in PJM and ERCOT.
* Show demand for the Northeast.

## ISO gas dependency

* How dependent is Texas on gas for power?
* Show gas burn in ERCOT.
* Show gas generation share during the cold snap.
* Compare gas dependency in ERCOT and PJM.
* How much gas was burned for electricity in Texas?

## ISO renewables

* Show clean energy generation in Texas.
* Show renewable penetration in CAISO.
* Show solar only in California.
* Compare renewables and gas share in PJM.
* Show intermittent generation in New England.

## ISO fuel mix

* What was on the stack in ERCOT?
* Show generation by source in California.
* Show the grid mix in Texas.
* Compare gas, coal, and nuclear in PJM.
* Show hourly mix excluding imports.

---
