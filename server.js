import express from 'express';
import cors from 'cors';
import multer from 'multer';
import path from 'path';
import fs from 'fs';
import pool, { initDb } from './db.js';

const app = express();
const PORT = process.env.PORT || 5000;

// Enable CORS and parsing of JSON/URL-encoded bodies
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Ensure upload directory exists
const uploadDir = './uploads';
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir, { recursive: true });
}

// Serve uploaded images static folder
app.use('/uploads', express.static(path.resolve(uploadDir)));

// Multer config for payment screenshot uploads
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, uploadDir);
  },
  filename: (req, file, cb) => {
    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1e9);
    cb(null, uniqueSuffix + path.extname(file.originalname));
  }
});
const upload = multer({ storage });

// Real-time SSE Clients
let adminClients = [];

function registerAdminClient(req, res) {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive'
  });

  const clientId = Date.now();
  const newClient = { id: clientId, res };
  adminClients.push(newClient);
  console.log(`Admin SSE client connected: ${clientId}. Total: ${adminClients.length}`);

  req.on('close', () => {
    adminClients = adminClients.filter(c => c.id !== clientId);
    console.log(`Admin SSE client disconnected: ${clientId}. Total: ${adminClients.length}`);
  });
}

function broadcastNotification(data) {
  console.log('Broadcasting notification:', data);
  adminClients.forEach(client => {
    client.res.write(`data: ${JSON.stringify(data)}\n\n`);
  });
}

// --- DATABASE AUTO MIGRATION RUN ---
// initDb is called inside startServer() below so the server only
// starts AFTER the database is fully initialised and the default
// admin credentials are guaranteed to exist.


// ==========================================
// ADMIN DASHBOARD API
// ==========================================

app.get('/api/admin/notifications/stream', registerAdminClient);

app.get('/api/admin/dashboard', async (req, res) => {
  try {
    // 1. Today's metrics (today start date)
    const today = new Date().toISOString().split('T')[0];

    // Today's orders count
    const [ordersCountRows] = await pool.query(
      "SELECT COUNT(*) as count FROM orders WHERE DATE(created_at) = ?", [today]
    );
    const todayOrders = ordersCountRows[0].count;

    // Today's revenue
    const [revenueRows] = await pool.query(
      "SELECT SUM(total_amount) as total FROM orders WHERE DATE(created_at) = ? AND status NOT IN ('Cancelled', 'Pending Payment', 'Pending Approval')",
      [today]
    );
    const todayRevenue = Number(revenueRows[0].total || 0);

    // Active orders count (Preparing or Ready)
    const [activeCountRows] = await pool.query(
      "SELECT COUNT(*) as count FROM orders WHERE status IN ('Preparing', 'Ready')"
    );
    const activeOrders = activeCountRows[0].count;

    // Pending approvals count
    const [pendingCountRows] = await pool.query(
      "SELECT COUNT(*) as count FROM orders WHERE status = 'Pending Approval'"
    );
    const pendingApprovals = pendingCountRows[0].count;

    // Occupied tables (status = Occupied)
    const [occupiedTablesRows] = await pool.query(
      "SELECT COUNT(*) as count FROM cafe_tables WHERE status = 'Occupied'"
    );
    const occupiedTablesCount = occupiedTablesRows[0].count;

    // Total tables
    const [totalTablesRows] = await pool.query(
      "SELECT COUNT(*) as count FROM cafe_tables"
    );
    const totalTables = totalTablesRows[0].count;

    // Total reviews today
    const [reviewsRows] = await pool.query(
      "SELECT COUNT(*) as count FROM reviews"
    );
    const totalReviews = reviewsRows[0].count;

    // 2. Revenue Trend (last 7 days)
    const [revTrendRows] = await pool.query(`
      SELECT DATE_FORMAT(created_at, '%a') as day, SUM(total_amount) as total
      FROM orders
      WHERE created_at >= DATE_SUB(CURDATE(), INTERVAL 6 DAY)
        AND status NOT IN ('Cancelled', 'Pending Payment', 'Pending Approval')
      GROUP BY DATE(created_at), day
      ORDER BY DATE(created_at) ASC
    `);

    // 3. Orders Trend (last 7 days)
    const [ordersTrendRows] = await pool.query(`
      SELECT DATE_FORMAT(created_at, '%a') as day, COUNT(*) as count
      FROM orders
      WHERE created_at >= DATE_SUB(CURDATE(), INTERVAL 6 DAY)
      GROUP BY DATE(created_at), day
      ORDER BY DATE(created_at) ASC
    `);

    // 4. Top Selling Items
    const [topSellingRows] = await pool.query(`
      SELECT item_name as name, SUM(quantity) as value
      FROM order_items oi
      JOIN orders o ON oi.order_id = o.id
      WHERE o.status NOT IN ('Cancelled')
      GROUP BY item_name
      ORDER BY value DESC
      LIMIT 5
    `);

    // 5. Category Performance
    const [catPerformanceRows] = await pool.query(`
      SELECT c.name, SUM(oi.quantity * oi.price) as value
      FROM order_items oi
      JOIN menu_items mi ON oi.menu_item_id = mi.id
      JOIN categories c ON mi.category_id = c.id
      JOIN orders o ON oi.order_id = o.id
      WHERE o.status NOT IN ('Cancelled')
      GROUP BY c.name
      ORDER BY value DESC
    `);

    // 6. Recent Orders (last 10)
    const [recentOrdersRows] = await pool.query(`
      SELECT id, order_number, table_number, customer_name, total_amount, status, created_at
      FROM orders
      ORDER BY created_at DESC
      LIMIT 10
    `);

    // 7. Recent Activity logs (simulated based on orders state history)
    const [recentActivityRows] = await pool.query(`
      SELECT 
        o.id,
        o.order_number,
        o.table_number,
        o.customer_name,
        o.status,
        o.updated_at as time
      FROM orders o
      ORDER BY o.updated_at DESC
      LIMIT 10
    `);
    
    const activities = recentActivityRows.map(act => {
      let message = '';
      if (act.status === 'Grace Period') message = `Order ${act.order_number} confirmed, grace period started for Table ${act.table_number}.`;
      else if (act.status === 'Pending Approval') message = `Payment submitted for Order ${act.order_number} (Table ${act.table_number}), pending approval.`;
      else if (act.status === 'Preparing') message = `Payment approved. Order ${act.order_number} (Table ${act.table_number}) is in Kitchen.`;
      else if (act.status === 'Ready') message = `Order ${act.order_number} for Table ${act.table_number} marked Ready.`;
      else if (act.status === 'Delivered') message = `Order ${act.order_number} delivered to Table ${act.table_number}.`;
      else if (act.status === 'Cancelled') message = `Order ${act.order_number} was Cancelled.`;
      else message = `Order ${act.order_number} status updated to ${act.status}.`;
      
      return {
        id: act.id,
        message,
        time: act.time
      };
    });

    res.json({
      metrics: {
        todayOrders,
        todayRevenue,
        activeOrders,
        pendingApprovals,
        occupiedTables: `${occupiedTablesCount}/${totalTables}`,
        totalReviews
      },
      charts: {
        revenueTrend: revTrendRows,
        ordersTrend: ordersTrendRows,
        topSelling: topSellingRows,
        categoryPerformance: catPerformanceRows
      },
      recentOrders: recentOrdersRows,
      recentActivity: activities
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database query failed' });
  }
});

// ==========================================
// ADMIN CATEGORIES API
// ==========================================

app.get('/api/admin/categories', async (req, res) => {
  try {
    const [rows] = await pool.query('SELECT * FROM categories ORDER BY display_order ASC');
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/admin/categories', async (req, res) => {
  const { name, slug, display_order, status } = req.body;
  if (!name || !slug) return res.status(400).json({ error: 'Name and slug are required' });
  try {
    const [result] = await pool.query(
      'INSERT INTO categories (name, slug, display_order, status) VALUES (?, ?, ?, ?)',
      [name, slug, display_order || 0, status || 'Active']
    );
    res.json({ id: result.insertId, name, slug, display_order, status });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/admin/categories/:id', async (req, res) => {
  const { id } = req.params;
  const { name, slug, display_order, status } = req.body;
  try {
    await pool.query(
      'UPDATE categories SET name = ?, slug = ?, display_order = ?, status = ? WHERE id = ?',
      [name, slug, display_order, status, id]
    );
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/admin/categories/:id', async (req, res) => {
  const { id } = req.params;
  try {
    await pool.query('DELETE FROM categories WHERE id = ?', [id]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ==========================================
// ADMIN MENU API
// ==========================================

app.get('/api/admin/menu', async (req, res) => {
  try {
    const [rows] = await pool.query(`
      SELECT m.*, c.name as category_name 
      FROM menu_items m 
      JOIN categories c ON m.category_id = c.id 
      ORDER BY c.display_order ASC, m.name ASC
    `);
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/admin/menu', upload.single('image'), async (req, res) => {
  const { name, category_id, price, description, is_available, image_url } = req.body;
  const imageUrl = req.file ? `/uploads/${req.file.filename}` : (image_url || '');
  
  if (!name || !category_id || !price) {
    return res.status(400).json({ error: 'Name, Category and Price are required' });
  }

  try {
    const [result] = await pool.query(
      'INSERT INTO menu_items (name, category_id, price, description, image_url, is_available) VALUES (?, ?, ?, ?, ?, ?)',
      [name, category_id, price, description, imageUrl, is_available === 'false' ? 0 : 1]
    );
    res.json({ id: result.insertId, success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/admin/menu/:id', upload.single('image'), async (req, res) => {
  const { id } = req.params;
  const { name, category_id, price, description, is_available, image_url } = req.body;
  
  let query = 'UPDATE menu_items SET name = ?, category_id = ?, price = ?, description = ?, is_available = ?';
  let params = [name, category_id, price, description, is_available === 'false' ? 0 : 1];

  if (req.file) {
    query += ', image_url = ?';
    params.push(`/uploads/${req.file.filename}`);
  } else if (image_url !== undefined) {
    query += ', image_url = ?';
    params.push(image_url);
  }
  query += ' WHERE id = ?';
  params.push(id);

  try {
    await pool.query(query, params);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/admin/menu/:id', async (req, res) => {
  const { id } = req.params;
  try {
    await pool.query('DELETE FROM menu_items WHERE id = ?', [id]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ==========================================
// ADMIN TABLES API
// ==========================================

app.get('/api/admin/tables', async (req, res) => {
  try {
    const [rows] = await pool.query('SELECT * FROM cafe_tables ORDER BY table_number ASC');
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/admin/tables', async (req, res) => {
  const { table_number, status } = req.body;
  if (!table_number) return res.status(400).json({ error: 'Table number is required' });
  try {
    await pool.query('INSERT INTO cafe_tables (table_number, status) VALUES (?, ?)', [table_number, status || 'Available']);
    res.json({ success: true, table_number });
  } catch (err) {
    res.status(500).json({ error: 'Table already exists or DB error: ' + err.message });
  }
});

app.put('/api/admin/tables/:table_number', async (req, res) => {
  const { table_number } = req.params;
  const { status } = req.body;
  try {
    await pool.query('UPDATE cafe_tables SET status = ? WHERE table_number = ?', [status, table_number]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/admin/tables/:table_number', async (req, res) => {
  const { table_number } = req.params;
  try {
    await pool.query('DELETE FROM cafe_tables WHERE table_number = ?', [table_number]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ==========================================
// ADMIN ORDERS & APPROVALS API
// ==========================================

// Get all orders (including kitchen flow filter)
app.get('/api/admin/orders', async (req, res) => {
  const { status } = req.query;
  let query = `
    SELECT o.*, GROUP_CONCAT(CONCAT(oi.item_name, ' x ', oi.quantity) SEPARATOR ', ') as items_summary 
    FROM orders o
    LEFT JOIN order_items oi ON o.id = oi.order_id
  `;
  const params = [];
  
  if (status && status !== 'All Statuses') {
    query += ' WHERE o.status = ?';
    params.push(status);
  } else {
    // By default, exclude Grace Period or Pending Payment from standard kitchen orders if not specified
    query += " WHERE o.status IN ('Preparing', 'Ready', 'Delivered', 'Cancelled')";
  }
  
  query += ' GROUP BY o.id ORDER BY o.created_at DESC';
  
  try {
    const [orders] = await pool.query(query, params);
    
    // Fetch full item details for each order
    for (const order of orders) {
      const [items] = await pool.query('SELECT * FROM order_items WHERE order_id = ?', [order.id]);
      order.items = items;
    }
    
    res.json(orders);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get pending approvals
app.get('/api/admin/approvals', async (req, res) => {
  try {
    const [orders] = await pool.query(`
      SELECT * FROM orders 
      WHERE status = 'Pending Approval' 
      ORDER BY payment_time DESC
    `);
    
    for (const order of orders) {
      const [items] = await pool.query('SELECT * FROM order_items WHERE order_id = ?', [order.id]);
      order.items = items;
    }
    
    res.json(orders);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Update order status (Preparing -> Ready -> Delivered, or Cancel)
app.put('/api/admin/orders/:id/status', async (req, res) => {
  const { id } = req.params;
  const { status } = req.body;
  
  try {
    // Retrieve table number first
    const [ord] = await pool.query('SELECT table_number, order_number FROM orders WHERE id = ?', [id]);
    if (ord.length === 0) return res.status(404).json({ error: 'Order not found' });
    
    await pool.query('UPDATE orders SET status = ? WHERE id = ?', [status, id]);
    
    // If status is Delivered, free up the table (make it Available)
    if (status === 'Delivered') {
      await pool.query('UPDATE cafe_tables SET status = "Available" WHERE table_number = ?', [ord[0].table_number]);
    }
    
    broadcastNotification({
      type: 'ORDER_STATUS_UPDATE',
      orderId: id,
      orderNumber: ord[0].order_number,
      tableNumber: ord[0].table_number,
      status,
      message: `Order ${ord[0].order_number} is now ${status}!`
    });
    
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Approve Pending Payment
app.put('/api/admin/orders/:id/approve', async (req, res) => {
  const { id } = req.params;
  try {
    const [ord] = await pool.query('SELECT table_number, order_number FROM orders WHERE id = ?', [id]);
    if (ord.length === 0) return res.status(404).json({ error: 'Order not found' });
    
    // Change order status to Preparing (Kitchen enters preparing mode)
    await pool.query('UPDATE orders SET status = "Preparing" WHERE id = ?', [id]);
    
    // Mark table as Occupied
    await pool.query('UPDATE cafe_tables SET status = "Occupied" WHERE table_number = ?', [ord[0].table_number]);
    
    broadcastNotification({
      type: 'PAYMENT_APPROVED',
      orderId: id,
      orderNumber: ord[0].order_number,
      tableNumber: ord[0].table_number,
      message: `Payment approved for Order ${ord[0].order_number}. Preparing starts.`
    });
    
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Reject Pending Payment
app.put('/api/admin/orders/:id/reject', async (req, res) => {
  const { id } = req.params;
  const { reason } = req.body;
  try {
    const [ord] = await pool.query('SELECT table_number, order_number FROM orders WHERE id = ?', [id]);
    if (ord.length === 0) return res.status(404).json({ error: 'Order not found' });
    
    // Update order status to Cancelled and save the rejection reason
    await pool.query('UPDATE orders SET status = "Cancelled", rejection_reason = ? WHERE id = ?', [reason || 'Payment rejected by cashier', id]);
    
    broadcastNotification({
      type: 'PAYMENT_REJECTED',
      orderId: id,
      orderNumber: ord[0].order_number,
      tableNumber: ord[0].table_number,
      reason: reason || 'Payment rejected by cashier',
      message: `Payment rejected for Order ${ord[0].order_number}. Reason: ${reason}`
    });
    
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ==========================================
// ADMIN REVIEWS API
// ==========================================

app.get('/api/admin/reviews', async (req, res) => {
  try {
    const [rows] = await pool.query('SELECT * FROM reviews ORDER BY created_at DESC');
    
    // Get average rating
    const [avgRow] = await pool.query('SELECT AVG(rating) as avg_rating FROM reviews');
    const average = Number(avgRow[0].avg_rating || 0).toFixed(1);
    
    res.json({ reviews: rows, average });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/admin/reviews/:id', async (req, res) => {
  const { id } = req.params;
  try {
    await pool.query('DELETE FROM reviews WHERE id = ?', [id]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ==========================================
// ADMIN REPORTS API
// ==========================================

app.get('/api/admin/reports', async (req, res) => {
  const { type, startDate, endDate } = req.query;
  let dateFilter = '';
  let dateFilterOrders = '';
  // Params for queries that use queryCondition (orders table only)
  const params = [];
  // Params for queries that use dateFilterOrders (order_items + orders join)
  const paramsOrders = [];

  if (type === 'daily') {
    dateFilter = 'DATE(created_at) = CURDATE()';
    dateFilterOrders = 'DATE(o.created_at) = CURDATE()';
  } else if (type === 'weekly') {
    dateFilter = 'created_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)';
    dateFilterOrders = 'o.created_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)';
  } else if (type === 'monthly') {
    dateFilter = 'created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)';
    dateFilterOrders = 'o.created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)';
  } else if (type === 'custom' && startDate && endDate) {
    dateFilter = 'DATE(created_at) BETWEEN ? AND ?';
    dateFilterOrders = 'DATE(o.created_at) BETWEEN ? AND ?';
    params.push(startDate, endDate);
    paramsOrders.push(startDate, endDate);
  } else {
    dateFilter = '1=1';
    dateFilterOrders = '1=1';
  }

  // Add condition to exclude cancelled and unapproved orders
  const queryCondition = `(${dateFilter}) AND status NOT IN ('Cancelled', 'Pending Payment', 'Pending Approval', 'Grace Period')`;
  const queryConditionOrders = `o.status NOT IN ('Cancelled', 'Pending Payment', 'Pending Approval', 'Grace Period') AND (${dateFilterOrders})`;

  const emptyResponse = {
    summary: { revenue: 0, ordersCount: 0 },
    ordersTrend: [],
    peakHours: [],
    categoryPerformance: [],
    bestSelling: [],
    topRevenue: []
  };

  try {
    // 1. Total Revenue
    const [rev] = await pool.query(
      `SELECT COALESCE(SUM(total_amount), 0) as value FROM orders WHERE ${queryCondition}`,
      params
    );
    const revenue = Number(rev[0]?.value || 0);

    // 2. Total Orders
    const [ord] = await pool.query(
      `SELECT COUNT(*) as value FROM orders WHERE ${queryCondition}`,
      params
    );
    const ordersCount = Number(ord[0]?.value || 0);

    // 3. Orders Trend (Graph data)
    // Use DATE(created_at) expression directly in GROUP BY instead of alias to avoid ONLY_FULL_GROUP_BY errors
    const [trend] = await pool.query(`
      SELECT
        DATE(created_at) as date,
        DATE_FORMAT(created_at, '%b %d') as formatted_date,
        COUNT(*) as count,
        COALESCE(SUM(total_amount), 0) as revenue
      FROM orders
      WHERE ${queryCondition}
      GROUP BY DATE(created_at), DATE_FORMAT(created_at, '%b %d')
      ORDER BY DATE(created_at) ASC
    `, params);

    // 4. Peak Hours
    const [peakHours] = await pool.query(`
      SELECT HOUR(created_at) as hour, COUNT(*) as count
      FROM orders
      WHERE ${queryCondition}
      GROUP BY HOUR(created_at)
      ORDER BY count DESC
    `, params);

    // 5. Category Performance
    const [catPerf] = await pool.query(`
      SELECT c.name, COALESCE(SUM(oi.quantity * oi.price), 0) as value
      FROM order_items oi
      JOIN menu_items mi ON oi.menu_item_id = mi.id
      JOIN categories c ON mi.category_id = c.id
      JOIN orders o ON oi.order_id = o.id
      WHERE ${queryConditionOrders}
      GROUP BY c.id, c.name
      ORDER BY value DESC
    `, paramsOrders);

    // 6. Best Selling Items (by Quantity)
    const [bestSelling] = await pool.query(`
      SELECT oi.item_name as name, COALESCE(SUM(oi.quantity), 0) as value
      FROM order_items oi
      JOIN orders o ON oi.order_id = o.id
      WHERE ${queryConditionOrders}
      GROUP BY oi.item_name
      ORDER BY value DESC
      LIMIT 10
    `, paramsOrders);

    // 7. Top Revenue Items
    const [topRevenue] = await pool.query(`
      SELECT oi.item_name as name, COALESCE(SUM(oi.quantity * oi.price), 0) as value
      FROM order_items oi
      JOIN orders o ON oi.order_id = o.id
      WHERE ${queryConditionOrders}
      GROUP BY oi.item_name
      ORDER BY value DESC
      LIMIT 10
    `, paramsOrders);

    return res.json({
      summary: {
        revenue: revenue,
        ordersCount: ordersCount
      },
      ordersTrend: trend || [],
      peakHours: peakHours || [],
      categoryPerformance: catPerf || [],
      bestSelling: bestSelling || [],
      topRevenue: topRevenue || []
    });
  } catch (err) {
    console.error('Reports API error:', err.message);
    return res.status(500).json({ ...emptyResponse, error: err.message });
  }
});

// ==========================================
// ADMIN SETTINGS API
// ==========================================

app.get('/api/admin/settings', async (req, res) => {
  try {
    const [cafe] = await pool.query('SELECT * FROM cafe_settings WHERE id = 1');
    const [printer] = await pool.query('SELECT * FROM printer_settings WHERE id = 1');
    res.json({
      cafe: cafe[0],
      printer: printer[0]
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/admin/settings/cafe', upload.single('logo'), async (req, res) => {
  const { cafe_name, address, phone, gst_number, gst_percentage, upi_id } = req.body;
  let query = 'UPDATE cafe_settings SET cafe_name = ?, address = ?, phone = ?, gst_number = ?, gst_percentage = ?, upi_id = ?';
  let params = [cafe_name, address, phone, gst_number, gst_percentage, upi_id];

  if (req.file) {
    query += ', logo_url = ?';
    params.push(`/uploads/${req.file.filename}`);
  }
  query += ' WHERE id = 1';

  try {
    await pool.query(query, params);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.put('/api/admin/settings/printer', async (req, res) => {
  const { printer_name, ip_address, port } = req.body;
  try {
    await pool.query(
      'UPDATE printer_settings SET printer_name = ?, ip_address = ?, port = ? WHERE id = 1',
      [printer_name, ip_address, port]
    );
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/admin/settings/printer/test', async (req, res) => {
  // Mock thermal printer connection test
  const { ip_address, port } = req.body;
  setTimeout(() => {
    res.json({ success: true, message: `Connected to printer at ${ip_address}:${port} successfully!` });
  }, 1000);
});


// ==========================================
// CUSTOMER CLIENT API
// ==========================================

// Get categories & available menu items
app.get('/api/customer/menu', async (req, res) => {
  try {
    const [categories] = await pool.query(`
  SELECT *
  FROM categories
  ORDER BY display_order ASC
`);

    const [items] = await pool.query(`
      SELECT
        m.*,
        c.name AS category_name
      FROM menu_items m
      JOIN categories c ON m.category_id = c.id
      WHERE m.is_available = 1
    `);

    res.json({ categories, items });
  }  catch (err) {
    console.error("===== MENU API ERROR =====");
    console.error(err);

    console.error("Code:", err.code);
    console.error("Message:", err.message);
    console.error("SQL Message:", err.sqlMessage);
    console.error("SQL:", err.sql);

    return res.status(500).json({
        error: err.message,
        code: err.code,
        sqlMessage: err.sqlMessage,
        sql: err.sql
    });
}
});

// Create Order (Grace Period starts)
app.post('/api/customer/order', async (req, res) => {
  const { tableNumber, customerName, customerMobile, items, notes } = req.body;
  
  if (!tableNumber || !customerName || !customerMobile || !items || items.length === 0) {
    return res.status(400).json({ error: 'Missing required order details' });
  }

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();

    // 1. Generate Order Number (e.g. KC-1008)
    const [maxIdRow] = await conn.query('SELECT MAX(id) as maxId FROM orders');
    const nextId = (maxIdRow[0].maxId || 0) + 1;
    const orderNumber = `KC-${1000 + nextId}`;

    // 2. Compute total amount
    let totalAmount = 0;
    for (const item of items) {
      const [mi] = await conn.query('SELECT price FROM menu_items WHERE id = ?', [item.id]);
      if (mi.length === 0) throw new Error(`Item ${item.name} not found`);
      totalAmount += mi[0].price * item.quantity;
    }

    // 3. Create Order in 'Grace Period' status
    const [orderResult] = await conn.query(
      `INSERT INTO orders (order_number, table_number, customer_name, customer_mobile, status, total_amount, notes) 
       VALUES (?, ?, ?, ?, 'Grace Period', ?, ?)`,
      [orderNumber, tableNumber, customerName, customerMobile, totalAmount, notes]
    );
    const orderId = orderResult.insertId;

    // 4. Save items
    for (const item of items) {
      const [mi] = await conn.query('SELECT price, name FROM menu_items WHERE id = ?', [item.id]);
      await conn.query(
        `INSERT INTO order_items (order_id, menu_item_id, item_name, quantity, price) 
         VALUES (?, ?, ?, ?, ?)`,
        [orderId, item.id, mi[0].name, item.quantity, mi[0].price]
      );
    }

    await conn.commit();
    
    // Broadcast notification to Admin
    broadcastNotification({
      type: 'NEW_ORDER_GRACE',
      orderId,
      orderNumber,
      tableNumber,
      customerName,
      message: `New Order ${orderNumber} placed by ${customerName} (Grace Period)`
    });

    res.json({ success: true, orderId, orderNumber });
  } catch (err) {
    await conn.rollback();
    res.status(500).json({ error: err.message });
  } finally {
    conn.release();
  }
});

// Get Order Details and track time remaining (20 seconds grace period)
app.get('/api/customer/orders/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const [ord] = await pool.query('SELECT * FROM orders WHERE id = ?', [id]);
    if (ord.length === 0) return res.status(404).json({ error: 'Order not found' });
    
    const [items] = await pool.query('SELECT * FROM order_items WHERE order_id = ?', [id]);
    
    // Calculate grace period time elapsed
    const createdTime = new Date(ord[0].created_at).getTime();
    const elapsedSeconds = (Date.now() - createdTime) / 1000;
    const remainingSeconds = Math.max(0, Math.floor(20 - elapsedSeconds));

    res.json({
      order: ord[0],
      items,
      remainingSeconds,
      isLocked: elapsedSeconds >= 20 || ord[0].status !== 'Grace Period'
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Edit Order during grace period
app.put('/api/customer/orders/:id/edit', async (req, res) => {
  const { id } = req.params;
  const { items, notes } = req.body;

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();

    // Verify order exists and is still in Grace Period
    const [ord] = await conn.query('SELECT created_at, status FROM orders WHERE id = ?', [id]);
    if (ord.length === 0) {
      conn.release();
      return res.status(404).json({ error: 'Order not found' });
    }

    const createdTime = new Date(ord[0].created_at).getTime();
    const elapsedSeconds = (Date.now() - createdTime) / 1000;
    if (elapsedSeconds >= 20 || ord[0].status !== 'Grace Period') {
      conn.release();
      return res.status(400).json({ error: 'Order is locked. Grace period has expired.' });
    }

    // Recompute total amount and clear old items
    let totalAmount = 0;
    await conn.query('DELETE FROM order_items WHERE order_id = ?', [id]);

    for (const item of items) {
      const [mi] = await conn.query('SELECT price, name FROM menu_items WHERE id = ?', [item.id]);
      totalAmount += mi[0].price * item.quantity;

      await conn.query(
        `INSERT INTO order_items (order_id, menu_item_id, item_name, quantity, price) 
         VALUES (?, ?, ?, ?, ?)`,
        [id, item.id, mi[0].name, item.quantity, mi[0].price]
      );
    }

    // Update order total and notes
    await conn.query(
      'UPDATE orders SET total_amount = ?, notes = ? WHERE id = ?',
      [totalAmount, notes || '', id]
    );

    await conn.commit();
    res.json({ success: true });
  } catch (err) {
    await conn.rollback();
    res.status(500).json({ error: err.message });
  } finally {
    conn.release();
  }
});

// Cancel entire order during grace period
app.delete('/api/customer/orders/:id/cancel', async (req, res) => {
  const { id } = req.params;
  try {
    const [ord] = await pool.query('SELECT created_at, status FROM orders WHERE id = ?', [id]);
    if (ord.length === 0) return res.status(404).json({ error: 'Order not found' });

    const createdTime = new Date(ord[0].created_at).getTime();
    const elapsedSeconds = (Date.now() - createdTime) / 1000;
    if (elapsedSeconds >= 20 || ord[0].status !== 'Grace Period') {
      return res.status(400).json({ error: 'Order is locked. Cannot cancel.' });
    }

    await pool.query('UPDATE orders SET status = "Cancelled" WHERE id = ?', [id]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Pay and complete (no screenshots)
app.post('/api/customer/orders/:id/pay', async (req, res) => {
  const { id } = req.params;
  try {
    const [ord] = await pool.query('SELECT * FROM orders WHERE id = ?', [id]);
    if (ord.length === 0) return res.status(404).json({ error: 'Order not found' });

    await pool.query(
      `UPDATE orders 
       SET status = 'Pending Approval', payment_time = NOW() 
       WHERE id = ?`,
      [id]
    );

    broadcastNotification({
      type: 'PAYMENT_PENDING',
      orderId: id,
      orderNumber: ord[0].order_number,
      tableNumber: ord[0].table_number,
      amount: ord[0].total_amount,
      customerName: ord[0].customer_name,
      message: `Payment submitted for ${ord[0].order_number} (Table ${ord[0].table_number}). Approve now.`
    });

    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Submit review after delivery
app.post('/api/customer/reviews', async (req, res) => {
  const { orderId, rating, reviewText } = req.body;
  if (!rating) return res.status(400).json({ error: 'Rating is required' });
  try {
    await pool.query(
      'INSERT INTO reviews (order_id, rating, review_text) VALUES (?, ?, ?)',
      [orderId || null, rating, reviewText || '']
    );
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


// ==========================================
// ADMIN AUTH & USERS API
// ==========================================

// Login endpoint
app.post('/api/admin/auth/login', async (req, res) => {
  const { email, password } = req.body;
  if (!email || !password) {
    return res.status(400).json({ error: 'Invalid email or password.' });
  }
  try {
    const normalizedEmail = email.trim().toLowerCase();
    const [rows] = await pool.query(
      'SELECT * FROM admin_users WHERE LOWER(email) = ? AND password = ?',
      [normalizedEmail, password]
    );
    if (rows.length === 0) {
      return res.status(401).json({ error: 'Invalid email or password.' });
    }
    res.json({ success: true, email: rows[0].email });
  } catch (err) {
    console.error('Login error:', err);
    res.status(500).json({ error: 'Server error. Please try again.' });
  }
});

// Get all admin users
app.get('/api/admin/users', async (req, res) => {
  try {
    const [rows] = await pool.query(
      'SELECT id, email, created_at FROM admin_users ORDER BY created_at DESC'
    );
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Add a new admin user
app.post('/api/admin/users', async (req, res) => {
  const { email, password } = req.body;
  if (!email || !password) {
    return res.status(400).json({ error: 'Email and password are required.' });
  }
  try {
    await pool.query(
      'INSERT INTO admin_users (email, password) VALUES (?, ?)',
      [email.trim().toLowerCase(), password]
    );
    res.json({ success: true });
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') {
      return res.status(409).json({ error: 'A user with this email already exists.' });
    }
    res.status(500).json({ error: err.message });
  }
});

// Delete an admin user
app.delete('/api/admin/users/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM admin_users WHERE id = ?', [req.params.id]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Serve React Frontend static files in Production Build
const clientBuildPath = path.resolve('./dist');
if (fs.existsSync(clientBuildPath)) {
  app.use(express.static(clientBuildPath));
  app.get('*', (req, res) => {
    res.sendFile(path.join(clientBuildPath, 'index.html'));
  });
}

// ==========================================
// STARTUP — DB first, then listen
// ==========================================
async function startServer() {
  try {
    await initDb();
    console.log('Database initialised successfully.');
  } catch (e) {
    console.error('FATAL: Database initialisation failed:', e);
    process.exit(1);
  }

  app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
    console.log('Admin login: kanchicafe@gmail.com / kanchi@123');
  });
}

startServer();
