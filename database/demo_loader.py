import random, logging
from datetime import date, timedelta

logger = logging.getLogger(__name__)
START_DATE = date(2023, 1, 1)
END_DATE   = date(2024, 12, 31)

def _rd(s=START_DATE, e=END_DATE):
    return s + timedelta(days=random.randint(0,(e-s).days))
def _ts(d):
    return f"{d} {random.randint(8,21):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
def _fm(d):
    m=d.month
    if m in(10,11):return random.uniform(1.6,2.2)
    if m in(12,1): return random.uniform(1.3,1.6)
    if m in(3,4):  return random.uniform(1.2,1.4)
    return random.uniform(0.85,1.1)
def _ph(conn):
    import sqlite3; return "?" if isinstance(conn,sqlite3.Connection) else "%s"
def _ex(conn,sql,p=()):  return conn.execute(sql,p)
def _lid(cur):
    if hasattr(cur,"lastrowid") and cur.lastrowid: return cur.lastrowid
    r=cur.fetchone()
    if r is None: return None
    return next(iter(r.values())) if isinstance(r,dict) else r[0]

CATEGORIES=[
    ("Staples & Grains","Rice, wheat, dal, flour and other staple food items"),
    ("Cooking Oils & Ghee","Refined oils, mustard oil, groundnut oil, ghee"),
    ("Spices & Masalas","Whole and powdered spices, masala blends"),
    ("Snacks & Namkeen","Packaged snacks, chips, biscuits, namkeen"),
    ("Beverages","Tea, coffee, juices, cold drinks, energy drinks"),
    ("Dairy & Eggs","Milk, curd, paneer, butter, cheese, eggs"),
    ("Personal Care","Soaps, shampoos, face wash, lotions, deodorants"),
    ("Home Cleaning","Detergents, floor cleaners, dishwash, mops"),
    ("Packaged Foods","Ready-to-eat, noodles, pasta, soups, sauces"),
    ("Confectionery","Chocolates, candies, sweets, chewing gum"),
    ("Health & Wellness","Vitamins, protein supplements, health drinks"),
    ("Pooja & Festival Items","Agarbatti, camphor, pooja essentials, diyas"),
    ("Stationery & Gifting","Pens, notebooks, gift items, greeting cards"),
    ("Tobacco & Pan","Cigarettes, pan masala (legal items)"),
]
SUPPLIERS=[
    ("Agro Fresh Distributors","Ramesh Yadav","9848012345","agrofresh@gmail.com","Hyderabad",5),
    ("Sri Venkatesh Wholesalers","Venkatesh Reddy","9848023456","svwholesale@gmail.com","Secunderabad",4),
    ("Laxmi Trading Company","Suresh Laxmi","9848034567","laxmitrading@gmail.com","Hyderabad",5),
    ("Hyderabad FMCG Depot","Mohammed Saleem","9848056789","hfmcg@gmail.com","Hyderabad",5),
    ("ITC Distributor Hyd","Kiran Kumar","9848067890","itchyd@gmail.com","Hyderabad",5),
    ("HUL Stockist Secbad","Priya Sharma","9848078901","hulstockist@gmail.com","Secunderabad",4),
    ("Nestle India Distributor","Rajan Mehta","9848089012","nestlehyd@gmail.com","Hyderabad",5),
    ("Britannia Depot Hyd","Sanjay Gupta","9848090123","britdepot@gmail.com","Hyderabad",4),
    ("Marico Distributor","Anita Verma","9848001234","maricodist@gmail.com","Hyderabad",4),
    ("Dabur India Stockist","Harish Nair","9848011235","daburnair@gmail.com","Hyderabad",5),
    ("Patanjali Depot Hyd","Balasubramaniam","9848022346","patanjalihyd@gmail.com","Hyderabad",3),
    ("Amul Depot Hyderabad","Geeta Pillai","9848066780","amulhyd@gmail.com","Hyderabad",5),
]
# (name,cat_idx,sup_idx,sku,cost,sell,stock,reorder)
PRODUCTS=[
    ("Sona Masoori Rice 5kg",0,0,"STG-001",195,230,120,30),
    ("Tata Salt 1kg",0,0,"STG-002",18,24,200,50),
    ("Aashirvaad Atta 5kg",0,0,"STG-003",195,230,90,25),
    ("Toor Dal 1kg",0,1,"STG-004",95,115,150,40),
    ("Chana Dal 1kg",0,1,"STG-005",85,105,130,35),
    ("Moong Dal 1kg",0,1,"STG-006",90,110,110,30),
    ("Besan 1kg",0,0,"STG-007",55,70,100,25),
    ("Fortune Sunflower Oil 1L",1,2,"COG-001",120,145,80,20),
    ("Amul Ghee 500ml",1,11,"COG-002",240,290,60,15),
    ("Saffola Gold Oil 1L",1,8,"COG-003",135,162,70,18),
    ("Patanjali Mustard Oil 1L",1,10,"COG-004",90,112,60,15),
    ("MDH Chana Masala 100g",2,3,"SPM-001",45,58,120,30),
    ("Everest Garam Masala 100g",2,3,"SPM-002",48,62,110,28),
    ("Red Chilli Powder 200g",2,0,"SPM-003",35,48,150,40),
    ("Turmeric Powder 200g",2,0,"SPM-004",30,42,140,35),
    ("Cumin Seeds 100g",2,1,"SPM-005",28,40,130,30),
    ("Lays Classic Salted 26g",3,3,"SNK-001",15,20,250,60),
    ("Haldiram Aloo Bhujia 400g",3,3,"SNK-002",95,120,100,25),
    ("Parle-G Biscuits 800g",3,7,"SNK-003",38,50,180,45),
    ("Britannia Marie Gold 300g",3,7,"SNK-004",32,42,160,40),
    ("Kurkure Masala Munch 80g",3,3,"SNK-005",18,25,200,50),
    ("Good Day Butter Cookies",3,7,"SNK-006",30,40,170,40),
    ("Tata Tea Premium 500g",4,4,"BEV-001",175,215,80,20),
    ("Nescafe Classic 50g",4,6,"BEV-002",95,120,70,18),
    ("Pepsi 2L",4,3,"BEV-003",55,75,120,30),
    ("Coca Cola 2L",4,3,"BEV-004",55,75,120,30),
    ("Real Fruit Juice 1L",4,3,"BEV-005",65,85,90,22),
    ("Bru Instant Coffee 50g",4,5,"BEV-006",85,108,75,18),
    ("Amul Full Cream Milk 1L",5,11,"DAI-001",52,64,100,30),
    ("Amul Butter 500g",5,11,"DAI-002",210,252,50,15),
    ("Eggs Tray of 30",5,1,"DAI-003",145,180,60,15),
    ("Dove Beauty Bar 75g",6,5,"PRC-001",38,52,150,35),
    ("Pantene Shampoo 340ml",6,5,"PRC-002",185,235,80,20),
    ("Dettol Soap 75g",6,5,"PRC-003",32,45,160,40),
    ("Colgate MaxFresh 150g",6,5,"PRC-004",65,85,140,35),
    ("Vaseline Lotion 200ml",6,5,"PRC-005",95,122,90,22),
    ("Surf Excel Matic 1kg",7,5,"HCL-001",180,225,100,25),
    ("Harpic Toilet Cleaner 1L",7,5,"HCL-002",88,115,90,22),
    ("Vim Dishwash Bar 155g",7,5,"HCL-003",25,35,180,45),
    ("Ariel Pods 8 count",7,5,"HCL-004",155,195,60,15),
    ("Maggi Noodles 70g",8,6,"PKF-001",12,16,300,75),
    ("MTR Ready Meal Dal Makhani",8,3,"PKF-002",62,82,90,22),
    ("Kissan Mixed Fruit Jam",8,5,"PKF-003",85,110,80,20),
    ("Heinz Tomato Ketchup 450g",8,3,"PKF-004",95,122,70,18),
    ("Dairy Milk Silk 60g",9,6,"CNF-001",65,85,150,38),
    ("KitKat 4 Finger 41.5g",9,6,"CNF-002",38,50,180,45),
    ("Mentos Mint Roll",9,6,"CNF-003",12,18,250,60),
    ("Horlicks Classic 500g",10,5,"HLT-001",275,340,55,14),
    ("Dabur Chyawanprash 1kg",10,9,"HLT-002",225,285,45,12),
    ("Complan Chocolate 500g",10,5,"HLT-003",295,365,50,12),
    ("Cycle Agarbatti Pack",11,4,"POJ-001",25,35,200,50),
    ("Mangaldeep Agarbatti 100g",11,4,"POJ-002",30,42,180,45),
    ("Camphor Tablets 50g",11,0,"POJ-003",22,32,150,38),
    ("Classmate Notebook 200pg",12,11,"STN-001",38,52,100,25),
    ("Reynolds Racer Pen 10pk",12,11,"STN-002",28,40,120,30),
    ("Classic Milds Cigarettes",13,4,"TOB-001",180,220,100,25),
    ("Vimal Pan Masala 15g",13,4,"TOB-002",10,15,400,100),
]
FNAMES=["Ramesh","Suresh","Mahesh","Rajesh","Dinesh","Ganesh","Naresh","Rakesh",
        "Priya","Kavya","Anjali","Pooja","Sneha","Divya","Rekha","Usha",
        "Mohammed","Abdul","Syed","Ibrahim","Farhan","Irfan","Salman","Imran",
        "Srinivas","Venkatesh","Balaji","Ravi","Anil","Vijay","Prasad","Kumar",
        "Arjun","Kiran","Rajan","Mohan","Deepak","Vivek","Alok","Amit",
        "Fatima","Ayesha","Zara","Nadia","Shabana","Sana","Meena","Sunita"]
LNAMES=["Sharma","Verma","Gupta","Singh","Kumar","Yadav","Patel","Shah",
        "Reddy","Naidu","Rao","Raju","Krishna","Murthy","Prasad","Babu",
        "Khan","Shaikh","Ansari","Siddiqui","Qureshi","Pasha","Malik","Hussain",
        "Iyer","Pillai","Nair","Varma","Desai","Trivedi","Mehta","Joshi"]
CITIES=["Hyderabad","Secunderabad","Dilsukhnagar","Kukatpally","Ameerpet",
        "Madhapur","Gachibowli","LB Nagar","Uppal","Nacharam",
        "Malakpet","Mehdipatnam","Tolichowki","Miyapur","Kompally"]
EXP_TMPL=[
    ("Rent",18000,0.00,1,"Monthly shop rent - Dilsukhnagar"),
    ("Salary",32000,0.05,2,"Staff salary (2 employees)"),
    ("Electricity",3200,0.25,5,"Electricity bill"),
    ("Internet",999,0.00,7,"ACT Fibernet monthly plan"),
    ("Transport",2500,0.30,10,"Delivery and stock transport"),
    ("Marketing",1500,0.40,15,"Pamphlets, local ads, offers"),
    ("Maintenance",1200,0.50,20,"Shop maintenance and repairs"),
    ("Packaging",1800,0.20,25,"Carry bags, wrapping material"),
    ("Miscellaneous",800,0.60,28,"Miscellaneous expenses"),
]


def has_any_data(conn, user_id):
    ph=_ph(conn)
    try:
        r=conn.execute(f"SELECT COUNT(*) FROM sales WHERE user_id={ph}",(user_id,)).fetchone()
        c=next(iter(r.values())) if isinstance(r,dict) else r[0]
        return int(c)>0
    except Exception:
        return False


def clear_demo_data(conn, user_id):
    ph=_ph(conn)
    tables=["stock_alerts","sale_items","purchase_items","sales","purchases",
            "products","customers","suppliers","categories","expenses","business_profiles"]
    try:
        for t in tables:
            _ex(conn,f"DELETE FROM {t} WHERE user_id={ph}",(user_id,))
        conn.commit()
        return {"success":True}
    except Exception as e:
        conn.rollback()
        return {"success":False,"error":str(e)}


def load_demo_data(conn, user_id):
    random.seed(user_id*1000+42)
    ph=_ph(conn)
    counts={}
    try:
        clear_demo_data(conn,user_id)

        # business_profiles
        _ex(conn,f"INSERT INTO business_profiles(user_id,business_name,business_type,gst_number,city,address) VALUES({ph},{ph},{ph},{ph},{ph},{ph})",
            (user_id,"Sharma General & Grocery Store","General Retail Store","36AABCS1234C1ZK","Hyderabad","Shop No. 14, Dilsukhnagar Main Road, Hyderabad"))
        counts["business_profiles"]=1

        # categories
        cat_ids=[]
        for name,desc in CATEGORIES:
            c=_ex(conn,f"INSERT INTO categories(user_id,name,description,created_at) VALUES({ph},{ph},{ph},{ph}) RETURNING id",
                  (user_id,name,desc,"2023-01-01 09:00:00"))
            cat_ids.append(_lid(c))
        counts["categories"]=len(cat_ids)

        # suppliers
        sup_ids=[]
        for name,cp,phone,em,city,rat in SUPPLIERS:
            c=_ex(conn,f"INSERT INTO suppliers(user_id,name,contact_person,phone,email,city,rating,created_at) VALUES({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph}) RETURNING id",
                  (user_id,name,cp,phone,em,city,rat,"2023-01-01 09:00:00"))
            sup_ids.append(_lid(c))
        counts["suppliers"]=len(sup_ids)

        # customers
        cust_ids=[]; ctypes=[]
        used=set()
        for i in range(250):
            if i % 50 == 0:
              conn.commit()
            fn=random.choice(FNAMES); ln=random.choice(LNAMES)
            b=f"{fn.lower()}.{ln.lower()}{i}"
            while b in used: b+=str(random.randint(1,9))
            used.add(b)
            ct=random.choices(["Retail","Wholesale","Corporate"],[0.70,0.20,0.10])[0]
            cr=_rd(START_DATE,START_DATE+timedelta(days=120))
            c=_ex(conn,f"INSERT INTO customers(user_id,name,phone,email,city,customer_type,created_at) VALUES({ph},{ph},{ph},{ph},{ph},{ph},{ph}) RETURNING id",
                  (user_id,f"{fn} {ln}",f"9{random.randint(600000000,999999999)}",f"{b}@gmail.com",random.choice(CITIES),ct,_ts(cr)))
            cust_ids.append(_lid(c)); ctypes.append(ct)
        counts["customers"]=len(cust_ids)

        # products
        prod_ids=[]; prod_meta=[]; sup2prod={}
        for i,(name,ci,si,sku,cost,sell,stock,reorder) in enumerate(PRODUCTS):
            c=_ex(conn,f"INSERT INTO products(user_id,name,category_id,supplier_id,sku,unit,cost_price,selling_price,current_stock,reorder_level,created_at) VALUES({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph}) RETURNING id",
                  (user_id,name,cat_ids[ci],sup_ids[si],f"{sku}-U{user_id}","pcs",cost,sell,stock,reorder,"2023-01-01 09:00:00"))
            pid=_lid(c); prod_ids.append(pid)
            prod_meta.append({"id":pid,"cost":cost,"sell":sell,"reorder":reorder})
            sup2prod.setdefault(si,[]).append(i)
        counts["products"]=len(prod_ids)

        # purchases + purchase_items
        np=0; npi=0
        for idx, pd in enumerate(sorted([_rd() for _ in range(180)])):
            if idx % 20 == 0:
                 conn.commit()
            si=random.randint(0,len(SUPPLIERS)-1)
            ch=random.sample(sup2prod.get(si,list(range(len(prod_meta)))),
                             min(random.randint(3,7),len(sup2prod.get(si,prod_meta))))
            total=0; irows=[]
            for idx in ch:
                pm=prod_meta[idx]; qty=random.randint(8,30)
                uc=round(pm["cost"]*random.uniform(0.93,1.0),2)
                total+=round(qty*uc,2); irows.append((pm["id"],qty,uc))
            c=_ex(conn,f"INSERT INTO purchases(user_id,supplier_id,purchase_date,total_amount,status,created_at) VALUES({ph},{ph},{ph},{ph},{ph},{ph}) RETURNING id",
                  (user_id,sup_ids[si],str(pd),round(total,2),"Delivered",_ts(pd)))
            pur_id=_lid(c); np+=1
            for pid2,qty,uc in irows:
                _ex(conn,f"INSERT INTO purchase_items(user_id,purchase_id,product_id,quantity,unit_cost,created_at) VALUES({ph},{ph},{ph},{ph},{ph},{ph})",
                    (user_id,pur_id,pid2,qty,uc,_ts(pd))); npi+=1
        counts["purchases"]=np; counts["purchase_items"]=npi

        # sales + sale_items
        PMS=["Cash","UPI","Card","Net Banking"]; PWS=[0.35,0.40,0.15,0.10]
        NOTES=["","","","","","","Regular customer","Festival offer","Bulk order","Home delivery","Corporate order"]
        all_dates=[]
        d=START_DATE
        while d<=END_DATE:
            fm=_fm(d); wk=1.3 if d.weekday()>=5 else 1.0
            n=max(1,min(int(random.gauss(8*fm*wk,2)),18))
            all_dates.extend([d]*n); d+=timedelta(days=1)
        all_dates=sorted(all_dates)
        ns=0; nsi=0
        for idx, sd in enumerate(all_dates):
            if idx % 100 == 0:
              conn.commit()
            ci=random.randint(0,79) if random.random()<0.65 else random.randint(0,len(cust_ids)-1)
            cid=cust_ids[ci]; ct=ctypes[ci]
            pm_m=random.choices(PMS,PWS)[0]; note=random.choice(NOTES)
            chosen=random.sample(prod_meta,min(random.choices([1,2,3,4],[0.32,0.32,0.22,0.14])[0],len(prod_meta)))
            fm=_fm(sd); total=0; irows=[]
            for pm in chosen:
                qty=random.choices([1,2,3,4,5],[0.38,0.28,0.18,0.10,0.06])[0]
                if random.random()<0.25: qty=max(1,int(qty*fm))
                price=round(pm["sell"]*random.uniform(0.98,1.02),2)
                dp=(random.choices([0,2,5],[0.76,0.17,0.07])[0] if ct=="Retail" else
                    random.choices([0,5,8,10,12],[0.15,0.25,0.28,0.20,0.12])[0] if ct=="Wholesale" else
                    random.choices([0,5,8,10],[0.26,0.35,0.24,0.15])[0])
                gross=round(qty*price,2); disc=round(gross*dp/100,2); sub=round(gross-disc,2)
                total+=sub; irows.append((pm["id"],qty,price,disc,sub))
            c=_ex(conn,f"INSERT INTO sales(user_id,customer_id,sale_date,total_amount,payment_method,notes,created_at) VALUES({ph},{ph},{ph},{ph},{ph},{ph},{ph}) RETURNING id",
                  (user_id,cid,str(sd),round(total,2),pm_m,note,_ts(sd)))
            sid=_lid(c); ns+=1
            for pid2,qty,price,disc,sub in irows:
                _ex(conn,f"INSERT INTO sale_items(user_id,sale_id,product_id,quantity,price,discount,subtotal,created_at) VALUES({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})",
                    (user_id,sid,pid2,qty,price,disc,sub,_ts(sd))); nsi+=1
        counts["sales"]=ns; counts["sale_items"]=nsi

        # expenses
        ec=0; cm=START_DATE.replace(day=1)
        while cm<=END_DATE.replace(day=1):
            conn.commit()
            for cat,base,var,day,desc in EXP_TMPL:
                try: ed=cm.replace(day=day)
                except ValueError: ed=cm.replace(day=28)
                if ed>END_DATE: continue
                fm=random.uniform(1.5,2.5) if cm.month in(10,11,12) and cat in("Marketing","Packaging") else 1.0
                amt=base*fm*random.uniform(1-var,1+var)
                if cat=="Salary" and cm.month==1 and cm.year==2024: amt*=1.08
                _ex(conn,f"INSERT INTO expenses(user_id,category,amount,expense_date,description,created_at) VALUES({ph},{ph},{ph},{ph},{ph},{ph})",
                    (user_id,cat,round(amt,2),str(ed),desc,_ts(ed))); ec+=1
            for _ in range(random.randint(1,2)):
                cat,lo,hi,desc=random.choice([("Maintenance",500,3000,"Emergency repair"),("Miscellaneous",200,1500,"Unexpected expense"),("Marketing",500,2000,"Special promotion"),("Packaging",300,800,"Extra packaging")])
                ed=_rd(cm,min(cm.replace(day=28),END_DATE))
                _ex(conn,f"INSERT INTO expenses(user_id,category,amount,expense_date,description,created_at) VALUES({ph},{ph},{ph},{ph},{ph},{ph})",
                    (user_id,cat,round(random.uniform(lo,hi),2),str(ed),desc,_ts(ed))); ec+=1
            cm=cm.replace(year=cm.year+1,month=1) if cm.month==12 else cm.replace(month=cm.month+1)
        counts["expenses"]=ec

        # stock_alerts
        for pm in prod_meta:
            _ex(conn,f"INSERT INTO stock_alerts(user_id,product_id,alert_type,threshold,is_active,created_at) VALUES({ph},{ph},{ph},{ph},{ph},{ph})",
                (user_id,pm["id"],"LOW_STOCK",pm["reorder"],1,"2023-01-01 09:00:00"))
        counts["stock_alerts"]=len(prod_meta)

        conn.commit()
        logger.info("Demo data loaded for user %s: %s",user_id,counts)
        return {"success":True,"counts":counts}
    except Exception as e:
        conn.rollback()
        logger.exception("Demo load failed user %s: %s",user_id,e)
        return {"success":False,"error":str(e)}