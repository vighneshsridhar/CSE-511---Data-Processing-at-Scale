package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // println ("test    test");
    // println ("\n");
    val rectangle_points = queryRectangle.split(",");
    // println ("test 1   test 1");
    val p1_x = rectangle_points(0).toDouble;
    val p1_y = rectangle_points(1).toDouble;
    val p2_x = rectangle_points(2).toDouble;
    val p2_y = rectangle_points(3).toDouble;
    
    // println ("test 2   test 2");
    val point = pointString.split(",");
    val p_x = point(0).toDouble;
    val p_y = point(1).toDouble;

    // println ("test 3   test 3");
    val left_x = p1_x.min(p2_x);
    val right_x = p1_x.max(p2_x);
    val bottom_y = p1_y.min(p2_y);
    val top_y = p1_y.max(p2_y);

    // println ("test 4   test 4");
    if (p_x >= left_x && p_x <= right_x && p_y >= bottom_y && p_y <= top_y){
        return true;    
    }

    // println ("test 5   test 5");

    return false; // YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART

}
