
----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Description:		Function check if property is accessible for the company, and return number of possible routes
-- Function:		isPropertyAccessible
-- Input params:	property_in / company_in
-- Output params:	number of routes from C0 to company_in for property_in, if company_in has access property_id; or zero if property is not-accessible
-- Target tables: 	graph_history
----------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION isPropertyAccessible(property_in integer, company_in integer) RETURNS integer AS $$
	DECLARE passed_routes INTEGER;
        BEGIN

		--------------------------------------------

		-- create history table, in order to keep track of edges unallocation // propconnect-exclusivity / tests / 06-x-unallocated		
		
		CREATE TABLE IF NOT EXISTS graph_history (
			nextval integer,					-- order seq number
			source integer,   					-- source company
			target integer,   					-- target company
			strength integer, 					-- "-1 "Xclusive or "1" Non-Xclusive
			property integer, 					-- property id
			PRIMARY KEY (nextval, source, target, strength, property));

		--------------------------------------------

		-- creating temporary dataset as *input_graph_dataset* which handle current allocations --

		DROP TABLE IF EXISTS input_graph_dataset;
		
		CREATE TEMP TABLE input_graph_dataset as
		SELECT  

			  prop_allocs.offering_company_id as source
			, prop_allocs.receiving_company_id as target
			, case when pg.exclusive = true then -1 else 1 end as strength
			, $1 as property 
			
		FROM property_grouping_properties pgp
		LEFT JOIN property_groupings pg ON pg.id = pgp.property_grouping_id
		LEFT JOIN allocation_items prop_items ON prop_items.property_grouping_id = pg.id
		LEFT JOIN allocations prop_allocs ON prop_allocs.id = prop_items.allocation_id

		WHERE
			pgp.property_id=$1
		ORDER BY prop_items.id ;

		--------------------------------------------

		-- update for new or removed nodes/edges into *graph_history* from *input_graph_dataset* --

		CREATE TEMP TABLE temporary_graph_history AS 
		SELECT source, target, strength, property FROM (
		SELECT igs.source, igs.target, igs.strength, igs.property
		FROM graph_history as igs
		INNER JOIN input_graph_dataset as igc
		ON igs.source=igc.source 
		AND igs.target=igc.target
		AND igs.strength=igc.strength
		AND igs.property=igc.property
		WHERE igc.property=$1 and igs.property=$1 
		
		UNION

		SELECT igc.source, igc.target, igc.strength, igc.property
		FROM graph_history as igs
		RIGHT JOIN input_graph_dataset as igc
		ON igs.source=igc.source 
		AND igs.target=igc.target
		AND igs.strength=igc.strength
		AND igs.property=igc.property
		WHERE igs.source is null and igc.property=$1 
		) as temproperty;

		--------------------------------------------
		
		-- filtering / cleaning graph_history for *property_in* based on Xclusivity rules (first part) --

		DELETE FROM graph_history where property=$1 ;

		DROP TABLE IF EXISTS temp_graph_history;   

		CREATE TEMP TABLE IF NOT EXISTS temp_graph_history (
							
			source integer,   				
			target integer,   					
			strength integer, 					
			property integer 					
			);

		truncate temp_graph_history;
		INSERT INTO temp_graph_history select * from temporary_graph_history where source in (select source from temporary_graph_history where property=$1 group by source having min(strength)=-1) and strength=-1 and property=$1 order by source, target;
		INSERT INTO temp_graph_history select * from temporary_graph_history where source not in (select source from temporary_graph_history where property=$1 group by source having min(strength)=-1) and property=$1 order by source, target;
		INSERT INTO graph_history  select row_number() OVER(order by source, target) AS nextval, source, target, strength, property  from temp_graph_history order by source, target;
		DROP TABLE IF EXISTS temp_graph_history; 
		
		DROP TABLE temporary_graph_history;
		DROP TABLE IF EXISTS input_graph_dataset;

		
		-- filtering/cleaning graph_history for *property_in* based on Xclusivity rules (second part) --
		
		DROP TABLE IF EXISTS temp_graph_cleaning_excl;

		CREATE TEMP TABLE temp_graph_cleaning_excl as
		SELECT
			  source
			, target
			, strength
			, property
		FROM (
		SELECT 
			 t1.source
			, t1.target
			, t1.strength
			, t2.source as pr_source
			, t2.target as pr_target
			, t2.strength as pr_strength
			, case when ((t2.strength=-1 and t1.source=t2.target) or t2.strength=1 or t2.strength is NULL) or (t1.source=t2.source and t1.strength=-1 and t2.strength=-1)then 1 else 0 end as active
			, t1.property
			, t1.nextval
		FROM (select * from graph_history where property=$1 order by nextval) as t1 
		LEFT JOIN (select * from graph_history where property=$1 order by nextval) as t2 
		ON t1.nextval-1= t2.nextval
		-- WHERE NOT (t1.source=t2.source and t1.strength=-1) or t1.source IS NULL
		) as temp
		where active=1
		order by nextval;

		
		DELETE FROM graph_history where property=$1 ;
		INSERT INTO graph_history SELECT row_number() OVER()  AS nextval, source, target, strength, property FROM temp_graph_cleaning_excl where source is not null and target is not null order by property, nextval; 

		DROP TABLE IF EXISTS temp_graph_cleaning_excl;
		--------------------------------------------

		-- search in graph source/target where target is *company_in*  

		WITH RECURSIVE graph AS (
		    SELECT source
			  ,target
			  ,',' || source::text || ',' || target::text || ',' AS path
			  ,1 AS depth
		    FROM   graph_history
		    WHERE  source = 0 and property=$1 

		    UNION ALL
		    SELECT o.source
			  ,o.target
			  ,g.path || o.target || ','
			  ,g.depth + 1
		    FROM   graph g
		    JOIN   graph_history o ON o.source = g.target
		    WHERE  g.path !~~ ('%,' || o.source::text || ',' || o.target::text || ',%') and property=$1 
		    )
		    
		SELECT  count(*) INTO passed_routes
		FROM    graph where target=$2;
	
                RETURN passed_routes;
        END;
$$ LANGUAGE plpgsql;

--------------------------- Example of execution -----------------------------

-- Cleaning database procedure:  truncate graph_history;
-- Assuming that root node has access for all properties

SELECT * FROM isPropertyAccessible(1,4);

-----------------------------------------------------------------------------
-- TEST CASES:

-- Level1: 1+ 2+
-- Leve12: 1+ 3+
-- Level3: 3+ 2+
-- level4: 4+ 3+ 2+ 
-- level5: 4+
-- level6: 2+ 3+ 4+

-- passed 1-6