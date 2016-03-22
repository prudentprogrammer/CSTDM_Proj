import cstdm_settings as cs
import psycopg2
import psycopg2.extras
import logging
import time
import pprint
import decimal

def getLogging():
  currentDate = time.strftime('%x').replace('/','_')
  fullPath = cs.LOGFILEPATH + 'run_' + currentDate + '.log'
  logging.basicConfig(filename=fullPath,level=logging.DEBUG, format='%(asctime)s %(message)s')
  logger = logging.getLogger('edge logger')
  return logger
  
def create_mega_table_intermediate():
  logger = getLogging()
  
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.mapit_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.mapit_port)+"'"
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  cursor = conn.cursor()
  
  createTableQuery = r"""
  DROP TABLE IF EXISTS network.mega_table_with_routes_intermediate;
  CREATE TABLE network.mega_table_with_routes_intermediate
  (
    "A" integer,
    "B" integer,
    regions int[],
    distances numeric[]
  ); 
  """
  
  cursor.execute(createTableQuery)
  conn.commit()
  
  logger.info("*" * 15)
  logger.info("Created Mega Table Intermediate")
  
def populate_mega_table_intermediate():
  logger = getLogging()

  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.mapit_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.mapit_port)+"'"
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  cursor = conn.cursor()
  
  
  rangeString = r"""
  SELECT MIN("Node"), MAX("Node")
  FROM input.nodes_with_regionids
  WHERE "TAZCentroid" = 1;
  """
  
  cursor.execute(rangeString)
  results = cursor.fetchall()
  minPoint = results[0][0]
  maxPoint = results[0][1]
  
  # The bad values are hard-coded for now
  badValues = [174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 288, 289, 290, 291, 292, 293, 294, 295, 296, 297, 298, 299, 349, 350, 351, 352, 353, 354, 355, 356, 357, 358, 359, 360, 361, 362, 363, 364, 365, 366, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376, 377, 378, 379, 380, 381, 382, 383, 384, 385, 386, 387, 388, 389, 390, 391, 392, 393, 394, 395, 396, 397, 398, 399, 472, 473, 474, 475, 476, 477, 478, 479, 480, 481, 482, 483, 484, 485, 486, 487, 488, 489, 490, 491, 492, 493, 494, 495, 496, 497, 498, 499, 547, 548, 549, 550, 551, 552, 553, 554, 555, 556, 557, 558, 559, 560, 561, 562, 563, 564, 565, 566, 567, 568, 569, 570, 571, 572, 573, 574, 575, 576, 577, 578, 579, 580, 581, 582, 583, 584, 585, 586, 587, 588, 589, 590, 591, 592, 593, 594, 595, 596, 597, 598, 599, 681, 682, 683, 684, 685, 686, 687, 688, 689, 690, 691, 692, 693, 694, 695, 696, 697, 698, 699, 782, 786, 787, 788, 792, 793, 794, 795, 796, 797, 798, 799, 1082, 1083, 1084, 1085, 1086, 1087, 1088, 1089, 1090, 1091, 1092, 1093, 1094, 1095, 1096, 1097, 1098, 1099, 1194, 1195, 1196, 1197, 1198, 1199, 1295, 1296, 1297, 1298, 1299, 1484, 1485, 1486, 1487, 1488, 1489, 1490, 1491, 1492, 1493, 1494, 1495, 1496, 1497, 1498, 1499, 1587, 1588, 1589, 1590, 1591, 1592, 1593, 1594, 1595, 1596, 1597, 1598, 1599, 1889, 1898, 1899, 2047, 2048, 2049, 2050, 2051, 2052, 2053, 2054, 2055, 2056, 2057, 2058, 2059, 2060, 2061, 2062, 2063, 2064, 2065, 2066, 2067, 2068, 2069, 2070, 2071, 2072, 2073, 2074, 2075, 2076, 2077, 2078, 2079, 2080, 2081, 2082, 2083, 2084, 2085, 2086, 2087, 2088, 2089, 2090, 2091, 2092, 2093, 2094, 2095, 2096, 2097, 2098, 2099, 2168, 2169, 2170, 2171, 2172, 2173, 2174, 2175, 2176, 2177, 2178, 2179, 2180, 2181, 2182, 2183, 2184, 2185, 2186, 2187, 2188, 2189, 2190, 2191, 2192, 2193, 2194, 2195, 2196, 2197, 2198, 2199, 2281, 2282, 2283, 2284, 2285, 2286, 2287, 2288, 2289, 2290, 2291, 2292, 2293, 2294, 2295, 2296, 2297, 2298, 2299, 2344, 2345, 2346, 2347, 2348, 2349, 2350, 2351, 2352, 2353, 2354, 2355, 2356, 2357, 2358, 2359, 2360, 2361, 2362, 2363, 2364, 2365, 2366, 2367, 2368, 2369, 2370, 2371, 2372, 2373, 2374, 2375, 2376, 2377, 2378, 2379, 2380, 2381, 2382, 2383, 2384, 2385, 2386, 2387, 2388, 2389, 2390, 2391, 2392, 2393, 2394, 2395, 2396, 2397, 2398, 2399, 2499, 2579, 2580, 2581, 2582, 2583, 2584, 2585, 2586, 2587, 2588, 2589, 2590, 2591, 2592, 2593, 2594, 2595, 2596, 2597, 2598, 2599, 2629, 2630, 2631, 2632, 2633, 2634, 2635, 2636, 2637, 2638, 2639, 2640, 2641, 2642, 2643, 2644, 2645, 2646, 2647, 2648, 2649, 2650, 2651, 2652, 2653, 2654, 2655, 2656, 2657, 2658, 2659, 2660, 2661, 2662, 2663, 2664, 2665, 2666, 2667, 2668, 2669, 2670, 2671, 2672, 2673, 2674, 2675, 2676, 2677, 2678, 2679, 2680, 2681, 2682, 2683, 2684, 2685, 2686, 2687, 2688, 2689, 2690, 2691, 2692, 2693, 2694, 2695, 2696, 2697, 2698, 2699, 2785, 2786, 2787, 2788, 2789, 2790, 2791, 2792, 2793, 2794, 2951, 2952, 2953, 2954, 2955, 2956, 2957, 2958, 2959, 2960, 2961, 2962, 2963, 2964, 2965, 2966, 2967, 2968, 2969, 2970, 2971, 2972, 2973, 2974, 2975, 2976, 2977, 2978, 2979, 2980, 2981, 2982, 2983, 2984, 2985, 2986, 2987, 2988, 2989, 2990, 2991, 2992, 2993, 2994, 2995, 2996, 2997, 2998, 2999, 3036, 3037, 3038, 3039, 3040, 3041, 3042, 3043, 3044, 3045, 3046, 3047, 3048, 3049, 3050, 3051, 3052, 3053, 3054, 3055, 3056, 3057, 3058, 3059, 3060, 3061, 3062, 3063, 3064, 3065, 3066, 3067, 3068, 3069, 3070, 3071, 3072, 3073, 3074, 3075, 3076, 3077, 3078, 3079, 3080, 3081, 3082, 3083, 3084, 3085, 3086, 3087, 3088, 3089, 3090, 3091, 3092, 3093, 3094, 3095, 3096, 3097, 3098, 3142, 3143, 3144, 3145, 3146, 3147, 3148, 3149, 3150, 3151, 3152, 3153, 3154, 3155, 3156, 3157, 3158, 3159, 3160, 3161, 3162, 3163, 3164, 3165, 3166, 3167, 3168, 3169, 3170, 3171, 3172, 3173, 3174, 3175, 3176, 3177, 3178, 3179, 3180, 3181, 3182, 3183, 3184, 3185, 3186, 3187, 3188, 3189, 3190, 3191, 3192, 3193, 3194, 3195, 3196, 3197, 3198, 3199, 3253, 3267, 3268, 3269, 3270, 3271, 3272, 3273, 3274, 3275, 3276, 3277, 3278, 3279, 3280, 3281, 3282, 3283, 3284, 3285, 3286, 3287, 3288, 3289, 3290, 3291, 3292, 3293, 3294, 3295, 3296, 3297, 3298, 3299, 3392, 3393, 3394, 3395, 3396, 3397, 3398, 3457, 3465, 3466, 3467, 3468, 3469, 3470, 3471, 3472, 3473, 3474, 3475, 3476, 3477, 3478, 3479, 3480, 3481, 3482, 3483, 3484, 3485, 3486, 3487, 3488, 3489, 3490, 3491, 3492, 3493, 3494, 3495, 3496, 3497, 3498, 3499, 3543, 3544, 3545, 3546, 3547, 3548, 3549, 3550, 3551, 3552, 3553, 3554, 3555, 3556, 3557, 3558, 3559, 3560, 3561, 3562, 3563, 3564, 3565, 3566, 3567, 3568, 3569, 3570, 3571, 3572, 3573, 3574, 3575, 3576, 3577, 3578, 3579, 3580, 3581, 3582, 3583, 3584, 3585, 3586, 3587, 3588, 3589, 3590, 3591, 3592, 3593, 3594, 3595, 3596, 3597, 3622, 3649, 3677, 3680, 3728, 3743, 3744, 3745, 3746, 3747, 3748, 3749, 3750, 3751, 3752, 3753, 3754, 3758, 3759, 3760, 3789, 3790, 3792, 3793, 3794, 3797, 3798, 3799, 3836, 3898, 3899, 3944, 3953, 3999, 4084, 4085, 4086, 4087, 4088, 4089, 4090, 4091, 4092, 4093, 4094, 4095, 4096, 4097, 4098, 4099, 4285, 4297, 4298, 4299, 4365, 4369, 4373, 4374, 4375, 4376, 4377, 4378, 4379, 4380, 4381, 4382, 4383, 4384, 4385, 4386, 4387, 4388, 4389, 4390, 4391, 4392, 4393, 4394, 4395, 4396, 4397, 4398, 4399, 4473, 4474, 4475, 4476, 4477, 4478, 4479, 4480, 4481, 4482, 4483, 4484, 4485, 4486, 4487, 4488, 4489, 4490, 4491, 4492, 4493, 4494, 4495, 4496, 4497, 4498, 4499, 4579, 4580, 4581, 4582, 4583, 4584, 4585, 4586, 4587, 4588, 4589, 4590, 4591, 4592, 4593, 4594, 4595, 4596, 4597, 4598, 4599, 4678, 4679, 4680, 4681, 4682, 4683, 4684, 4685, 4686, 4687, 4688, 4689, 4690, 4691, 4692, 4693, 4694, 4695, 4696, 4697, 4698, 4699, 4781, 4782, 4783, 4784, 4785, 4786, 4787, 4788, 4789, 4790, 4791, 4792, 4793, 4794, 4795, 4796, 4797, 4798, 4799, 4844, 4845, 4846, 4847, 4876, 4997, 4998, 4999, 5094, 5181, 5186, 5187, 5188, 5189, 5190, 5191, 5192, 5193, 5297, 5298, 5299, 5368, 5386, 5397, 5474, 5495, 5708, 5716, 5717, 5718, 5745, 5776, 5777, 5883, 5885, 5886, 6058, 6059, 6060, 6061, 6062, 6063, 6064, 6065, 6066, 6067, 6068, 6069, 6070, 6071, 6072, 6073, 6074, 6075, 6076, 6077, 6078, 6079, 6080, 6081, 6082, 6083, 6084, 6085, 6086, 6087, 6088, 6089, 6090, 6091, 6092, 6093, 6094, 6095, 6096, 6097, 6138, 6139, 6140, 6141, 6142, 6143, 6144, 6145, 6146, 6147, 6148, 6149, 6150, 6151, 6152, 6153, 6154, 6155, 6156, 6157, 6158, 6159, 6160, 6161, 6162, 6163, 6164, 6165, 6166, 6167, 6168, 6169, 6170, 6171, 6172, 6173, 6174, 6175, 6176, 6177, 6178, 6179, 6180, 6181, 6182, 6183, 6184, 6185, 6186, 6187, 6188, 6189, 6190, 6191, 6192, 6193, 6194, 6195, 6196, 6197, 6198, 6199, 6451, 6476, 6477, 6478, 6479, 6480, 6481, 6482, 6483, 6484, 6485, 6486, 6487, 6488, 6489, 6490, 6491, 6492, 6493, 6494, 6495, 6496, 6497, 6498, 6499, 6588, 6589, 6590, 6591, 6592, 6593, 6594, 6595, 6596, 6597, 6598, 6599, 6600, 6613, 6651, 6673, 6674, 6675, 6676, 6677, 6678, 6679, 6680, 6681, 6682, 6683, 6684, 6685, 6686, 6687, 6688, 6689, 6690, 6691, 6692, 6693, 6694, 6695, 6696, 6697, 6698, 6699, 6707, 6708, 6709, 6710, 6711, 6712, 6713, 6714, 6715, 6716, 6717, 6718, 6719, 6720, 6721, 6722, 6723, 6724, 6725, 6726, 6727, 6728, 6729, 6730, 6731, 6732, 6733, 6734, 6735, 6736, 6737, 6738, 6739, 6740, 6741, 6742, 6743, 6744, 6745, 6746, 6747, 6748, 6749, 6750, 6751, 6752, 6753, 6754, 6755, 6756, 6757, 6758, 6759, 6760, 6761, 6762, 6763, 6764, 6765, 6766, 6767, 6768, 6769, 6770, 6771, 6772, 6773, 6774, 6775, 6776, 6777, 6778, 6779, 6780, 6781, 6782, 6783, 6784, 6785, 6786, 6787, 6788, 6789, 6790, 6791, 6792, 6793, 6794, 6795, 6796, 6797, 6798, 6799, 6823, 6841, 6842, 6843, 6844, 6845, 6846, 6847, 6848, 6849, 6850, 6851, 6852, 6853, 6854, 6855, 6856, 6857, 6858, 6859, 6860, 6861, 6862, 6863, 6864, 6865, 6866, 6867, 6868, 6869, 6870, 6871, 6872, 6873, 6874, 6875, 6876, 6877, 6878, 6879, 6880, 6881, 6882, 6883, 6884, 6885, 6886, 6887, 6888, 6889, 6890, 6891, 6892, 6893, 6894, 6895, 6896, 6897, 6898, 6899]
  badValues = set(badValues)
  listRanges = set(list(range(minPoint, maxPoint+1)))
  goodValues = list(listRanges - badValues)
  
  # Iterate only over the good values
  for i in goodValues:
    listOfTuples = []
    
    # execute the shortest path query
    '''
    shortestPathQuery = r"""
    SELECT dest_group, array_agg("RegionID") as regions, array_agg(total_cost) AS total_costs FROM
    (
      SELECT dest_group, initcap("REGION") AS region, SUM(cost) AS total_cost
      FROM input.node_links
      INNER JOIN
      (
       SELECT id1 as dest_group, source, target, input.edge_table.cost FROM pgr_kdijkstraPath(
       'SELECT id, source, target, cost FROM network.edge_table',
       %s, array[%s], false, false)
       INNER JOIN input.edge_table ON (id3 = id)
      ) R1
      ON (R1.source = "A") AND (R1.target = "B")
      GROUP BY dest_group, region
    ) R2
    INNER JOIN input.regionid_to_regions
    ON ("Region" = region)
    GROUP BY dest_group
    ORDER BY dest_group;
    """ % (str(i), str(goodValues).strip('[]'))
    '''
    shortestPathQuery = r"""
    SELECT dest_group, array_agg("RegionID") as regions, array_agg(total_cost) AS total_costs FROM
    (
      SELECT dest_group, "RegionID", sum(cost) AS total_cost, (array_agg(seq ORDER BY seq ASC))[1] as seq_ord FROM
      (
        SELECT seq, id1 as dest_group, source, target, network.edge_table.cost FROM pgr_kdijkstraPath(
        'SELECT id, source, target, cost FROM network.edge_table',
        %s, array[%s], false, false)
        INNER JOIN network.edge_table ON (id3 = id)
        ORDER BY seq
      ) R1
      INNER JOIN input.node_links_with_regionids
      ON (R1.source = "A") AND (R1.target = "B")
      GROUP BY dest_group, "RegionID"
      ORDER BY seq_ord
    ) R2
    GROUP BY dest_group
    ORDER BY dest_group;
    """ % (str(i), str(goodValues).strip('[]'))

    cursor.execute(shortestPathQuery)
    # retrieve the vertices along with their regions  from the database
    records_regions = cursor.fetchall()
      
    for dataPoint in records_regions:
      listOfTuples.append((i, dataPoint[0], dataPoint[1], dataPoint[2]))
   
    args_str = ','.join(['%s' for t in listOfTuples])
    insert_query = 'INSERT INTO network.mega_table_with_routes_intermediate VALUES {0}'.format(args_str)
    cursor.execute(insert_query, listOfTuples)
    print str(i) + "ith insert done"
    conn.commit()
    
  logger.info("*" * 15)
  logger.info("Populated Mega Table Intermediate")
  
  # Redo the steps again but only for ETM Ranges
  etm_ranges = list(range(1, 51+1))
  for i in etm_ranges:
    listOfTuples = []
    
    # execute the shortest path query
    shortestPathQuery = r"""
    SELECT dest_group, array_agg("RegionID") as regions, array_agg(total_cost) AS total_costs FROM
    (
      SELECT dest_group, "RegionID", sum(cost) AS total_cost, (array_agg(seq ORDER BY seq ASC))[1] as seq_ord FROM
      (
        SELECT seq, id1 as dest_group, source, target, network.edge_table.cost FROM pgr_kdijkstraPath(
        'SELECT id, source, target, cost FROM network.edge_table',
        %s, array[%s], false, false)
        INNER JOIN network.edge_table ON (id3 = id)
        ORDER BY seq
      ) R1
      INNER JOIN input.node_links_with_regionids
      ON (R1.source = "A") AND (R1.target = "B")
      GROUP BY dest_group, "RegionID"
      ORDER BY seq_ord
    ) R2
    GROUP BY dest_group
    ORDER BY dest_group;
    """ % (str(i), str(goodValues).strip('[]'))
    #print shortestPathQuery
    cursor.execute(shortestPathQuery)
    # retrieve the vertices along with their regions from the database
    records_regions = cursor.fetchall()
      
    for dataPoint in records_regions:
      listOfTuples.append((i, dataPoint[0], dataPoint[1], dataPoint[2]))
   
    args_str = ','.join(['%s' for t in listOfTuples])
    insert_query = 'INSERT INTO network.mega_table_with_routes_intermediate VALUES {0}'.format(args_str)
    cursor.execute(insert_query, listOfTuples)
    print str(i) + "ith insert done for ETM"
    conn.commit()
    
  logger.info("*" * 15)
  logger.info("Populated Mega Table Intermediate for ETM ranges")
  
def create_mega_table_final():
  logger = getLogging()
  
  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.mapit_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.mapit_port)+"'"
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  cursor = conn.cursor()
  
  createTableQuery = r"""
  DROP TABLE IF EXISTS network.mega_table_with_routes;
  CREATE TABLE network.mega_table_with_routes
  (
    "A" integer,
    "B" integer,
    "R1" numeric,
    "R2" numeric,
    "R3" numeric,
    "R4" numeric,
    "R5" numeric,
    "R6" numeric,
    "R7" numeric,
    "R8" numeric,
    "R9" numeric,
    route_orders numeric[]
  ); 
  """
  
  cursor.execute(createTableQuery)
  conn.commit()
  
  logger.info("*" * 15)
  logger.info("Created Final Mega Table")
  
def populate_mega_table_final():
  logger = getLogging()

  conn_string = "host='"+cs.pg_host+"' dbname='"+cs.mapit_database+"' user='"+cs.pg_user+"' password='"+cs.pg_password+"' port='"+str(cs.mapit_port)+"'"
  # get a connection, if a connect cannot be made an exception will be raised here
  conn = psycopg2.connect(conn_string)
  # conn.cursor will return a cursor object, you can use this cursor to perform queries
  cursor = conn.cursor(name='super_cursor', withhold=True)
  
  cursor.execute("SELECT * FROM network.mega_table_with_routes_intermediate;");
  print 'Creating the Final Mega Table...'
  while True:
    rows = cursor.fetchmany(100000)
    print("Transferring 100000 rows...")
    listOfTuples = []
    if not rows:
      break
    cursor2 = conn.cursor()
    
    # Correspondence
    # row[0] -> Source or A
    # row[1] -> Dest or B
    # row[2] -> Regions
    # row[3] -> Distance Costs
    
    for row in rows:  
      # 9 because there are 9 regions
      gl_list = [0] * 9
      temp = 0
      # Map the regions to the indices in the python list
      # After that, insert the list into the database
      # ex:- row[2] = {7, 9, 2, 1}
      # gl_list = [20, 76, 0, 0, 0, 0, 5, 0, 7]
      for i in row[2]:
        gl_list[i - 1] = float(str(decimal.Decimal(row[3][temp])))
        temp += 1
      individual_row = (row[0], row[1], gl_list[0], gl_list[1], gl_list[2], gl_list[3], gl_list[4], gl_list[5], gl_list[6], gl_list[7], gl_list[8], str(row[2]).replace('[', '{').replace(']','}'))
      listOfTuples.append(individual_row)
    
    args_str = str(listOfTuples).strip('[]')
    insertQuery = 'INSERT INTO network.mega_table_with_routes VALUES '+ args_str
    cursor2.execute(insertQuery)
    conn.commit()
      
  logger.info("*" * 15)
  logger.info("Populated Final Mega Table")
  
if __name__ == "__main__":
  create_mega_table_intermediate()
  populate_mega_table_intermediate()
  create_mega_table_final()
  populate_mega_table_final()
  print("Done creating the mega table.")