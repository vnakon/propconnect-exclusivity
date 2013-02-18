
-----------------------------------------------------------------------------------------------------------------------------------------
-- Function:	isPropertyAccessible
-- Input:	property_in / company_in
-- Output:	number of routes from C0 to company_in for property_in, if company_in has access property_id; or zero if not-accessible
-----------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION isPropertyAccessible(property_in integer, company_in integer) RETURNS integer AS $$
	DECLARE passed_routes INTEGER;
        BEGIN

		--------------------------------------------

		-- create history table, in order to keep track of edges unallocation // propconnect-exclusivity / tests / 06-x-unallocated		
		
		CREATE TABLE IF NOT EXISTS graph_history (
			nextval integer,					-- order seq number
			source integer,   					-- source company
			target integer,   					-- target company
			strength integer, 					-- Xclusive or Non-Xclusive
			property integer 					-- property id
			--, PRIMARY KEY(nextval,source,target,strength, property)
			);

		--------------------------------------------

		-- creating temporary dataset as *input_graph_dataset* which handle current allocations, will be removed later

		DROP TABLE IF EXISTS input_graph_dataset;
		
		CREATE TEMP TABLE input_graph_dataset as
		SELECT  
			  row_number() OVER(ORDER BY prop_items.id) AS nextval
			, prop_allocs.offering_company_id as source
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

		-- update data into *graph_history* from *input_graph_dataset* 

		CREATE TEMP TABLE temporary_graph_history as SELECT row_number() OVER(ORDER BY nextval_temp) AS nextval, source, target, strength, property FROM (
		SELECT row_number() OVER() AS nextval_temp, igs.*
		FROM graph_history as igs
		INNER JOIN input_graph_dataset as igc
		ON igs.source=igc.source 
		AND igs.target=igc.target
		AND igs.strength=igc.strength
		AND igs.property=igc.property
		WHERE igc.property=$1 and igs.property=$1 
		
		UNION

		SELECT 100+row_number() OVER() AS nextval_temp, igc.*
		FROM graph_history as igs
		RIGHT JOIN input_graph_dataset as igc
		ON igs.source=igc.source 
		AND igs.target=igc.target
		AND igs.strength=igc.strength
		AND igs.property=igc.property
		WHERE igs.source is null and igc.property=$1 
		) as temproperty  
		ORDER BY nextval_temp;



		DELETE FROM graph_history where property=$1 ;
		INSERT INTO graph_history SELECT * FROM temporary_graph_history order by property, nextval;
		DROP TABLE temporary_graph_history;
		DROP TABLE IF EXISTS input_graph_dataset;
		--------------------------------------------

		-- cleaning input data-set for *property_in* based on Xclusivity rules
		
		DROP TABLE IF EXISTS temp_graph_cleaning_excl;

		--CREATE TEMP TABLE temp_graph_cleaning_excl as
		CREATE TABLE temp_graph_cleaning_excl as
		SELECT
			 source
			,target
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
	
		) as temp
		where active=1
		order by nextval;

		-------------------------------------------------

		DROP TABLE IF EXISTS temp_graph_cleaning_cycle;

		--CREATE TEMP TABLE temp_graph_cleaning_cycle as 
		CREATE TABLE temp_graph_cleaning_cycle as 
		SELECT
		   t1.source
		 , t1.target
		 , t1.strength
		 , t1.property
		 , t1.nextval
		FROM (
		select row_number() OVER()  AS nextval, * from temp_graph_cleaning_excl where property=$1 ) as t1
		LEFT JOIN (
		select row_number() OVER()  AS nextval, * from temp_graph_cleaning_excl where property=$1 ) as t2
		ON t1.nextval+1=t2.nextval

		WHERE NOT (t1.source=t2.source and t2.strength=-1) or t2.source IS NULL  ORDER by t1.nextval ;


		----- update graph_history, in order to keep track of removes, case #6
		
		DELETE FROM graph_history where property=$1 ;
		INSERT INTO graph_history SELECT row_number() OVER()  AS nextval, source, target, strength, property FROM temp_graph_cleaning_cycle order by property, nextval; 

		--> new test
		DROP TABLE IF EXISTS temp_graph_cleaning_excl2;

		--CREATE TEMP TABLE temp_graph_cleaning_excl2 as
		CREATE TABLE temp_graph_cleaning_excl2 as
		SELECT
			 source
			,target
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
	
		) as temp
		where active=1
		order by nextval;

		DELETE FROM graph_history where property=$1 ;
		INSERT INTO graph_history SELECT row_number() OVER()  AS nextval, source, target, strength, property FROM temp_graph_cleaning_excl2 order by property, nextval;
		-------------------------------------------------

		-- doing search in graph source/target where target is *company_in*  

		WITH RECURSIVE graph AS (
		    SELECT source
			  ,target
			  ,',' || source::text || ',' || target::text || ',' AS path
			  ,1 AS depth
		    FROM   temp_graph_cleaning_cycle
		    WHERE  source = 0

		    UNION ALL
		    SELECT o.source
			  ,o.target
			  ,g.path || o.target || ','
			  ,g.depth + 1
		    FROM   graph g
		    JOIN   temp_graph_cleaning_cycle o ON o.source = g.target
		    WHERE  g.path !~~ ('%,' || o.source::text || ',' || o.target::text || ',%')
		    )
		    
		SELECT  count(*) INTO passed_routes
		FROM    graph where target=$2;
		
		----------------------------------------------------
		--DROP TABLE IF EXISTS temp_graph_cleaning_excl;
		--DROP TABLE IF EXISTS temp_graph_cleaning_cycle;
		----------------------------------------------------
		
                RETURN passed_routes;
        END;
$$ LANGUAGE plpgsql;

--------------------------- EXECUTE -----------------------------
-- Cleaning database procedure:  truncate graph_history;

SELECT * FROM isPropertyAccessible(1,3);

-- test: + 01 level -> passed + + +
-- test: + 02 level -> passed + + +
-- test: + 03 level -> passed + + +
-- test: + 04 level -> passed + + +
-- test: + 05 level -> passed + + +
-- test: + 06 level -> passed + + +