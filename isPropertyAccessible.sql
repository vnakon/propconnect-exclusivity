
CREATE OR REPLACE FUNCTION isPropertyAccessible(property_in integer, company_in integer) RETURNS integer AS $$
	DECLARE passed INTEGER;
        BEGIN
        
		--------------------------------------------

		-- creating temporary dataset as input graph structure
		
		DROP TABLE IF EXISTS input_graph;

		CREATE TABLE input_graph as
		SELECT  
			row_number() OVER(ORDER BY prop_items.id) AS nextval
			, prop_items.id as or_id
			, pgp.property_id
			, pgp.property_grouping_id 
			, pg.name
			, pg.exclusive
			, prop_allocs.offering_company_id as source
			, prop_allocs.receiving_company_id as target
			, case when pg.exclusive = true then -1 else 1 end as strength
			
		FROM property_grouping_properties pgp
		JOIN property_groupings pg ON pg.id = pgp.property_grouping_id
		JOIN allocation_items prop_items ON prop_items.property_grouping_id = pg.id
		JOIN allocations prop_allocs ON prop_allocs.id = prop_items.allocation_id

		WHERE
			pgp.property_id=$1
		ORDER BY prop_items.id ;

		--------------------------------------------

		-- cleaning input data-set for choosen *property_in* based on Xclusivity rules
		
		DROP TABLE IF EXISTS input_graph_cleaned;

		CREATE TABLE input_graph_cleaned as
		SELECT
			 source
			,target
			, strength
		FROM (
		SELECT 
			t1.or_id
			, t1.source
			, t1.target
			, t1.strength
			, t2.source as pr_source
			, t2.target as pr_target
			, t2.strength as pr_strength
			, case when ((t2.strength=-1 and t1.source=t2.target) or t2.strength=1 or t2.strength is NULL) or (t1.source=t2.source and t1.strength=-1 and t2.strength=-1)then 1 else 0 end as active
		FROM input_graph as t1 
		LEFT JOIN input_graph as t2 
		ON t1.nextval-1= t2.nextval

		) as temp
		where active=1;

		-------------------------------------------------

		DROP TABLE IF EXISTS input_graph_cleaned_cycle;

		create table input_graph_cleaned_cycle as 
		SELECT
		   t1.source
		 , t1.target
		 , t1.strength
		FROM (
		select row_number() OVER()  AS nextval, * from input_graph_cleaned ) as t1
		LEFT JOIN (
		select row_number() OVER()  AS nextval, * from input_graph_cleaned ) as t2
		ON t1.nextval+1=t2.nextval

		WHERE NOT (t1.source=t2.source and t2.strength=-1) or t2.source IS NULL ;

		-------------------------------------------------

		-- doing search in graph source/target where target is *company_in*  

		WITH RECURSIVE graph AS (
		    SELECT source
			  ,target
			  ,',' || source::text || ',' || target::text || ',' AS path
			  ,1 AS depth
		    FROM   input_graph_cleaned_cycle
		    WHERE  source = 0

		    UNION ALL
		    SELECT o.source
			  ,o.target
			  ,g.path || o.target || ','
			  ,g.depth + 1
		    FROM   graph g
		    JOIN   input_graph_cleaned_cycle o ON o.source = g.target
		    WHERE  g.path !~~ ('%,' || o.source::text || ',' || o.target::text || ',%')
		    )
		    
		SELECT  count(*) INTO passed
		FROM    graph where target=$2;
		
		----------------------------------------------------
		DROP TABLE IF EXISTS input_graph;
		DROP TABLE IF EXISTS input_graph_cleaned;
		DROP TABLE IF EXISTS input_graph_cleaned_cycle;
		----------------------------------------------------
		
                RETURN passed;
        END;
$$ LANGUAGE plpgsql;

------------ EXECUTE -------------------

SELECT * FROM isPropertyAccessible(1,4);

-- test: + 01 level -> passed
-- test: + 02 level -> passed
-- test: + 03 level -> passed
-- test: + 04 level -> passed
-- test: + 05 level -> passed