\-- TOTAL CUMILATIVE NUMBER OF TIGO USERS
SELECT substring(mpa.msisdn, 1,3), count(DISTINCT mpa.uid) 
FROM bs_mobile_partner_accounts mpa 
LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id 
LEFT JOIN users u ON u.uid = mpa.uid 
WHERE mpa.uid is not null 
AND mpas.start <= '%s' 
AND mpa.partner = 'tigo' 
AND mpas.provider_plan != 2 
AND mpas.current = 1 
group by 1;

-- # TOTAL NUMBER OF NEW USERS WITH CONFIRMED EMAIL
SELECT substring(mpa.msisdn, 1,3), count(DISTINCT mpa.msisdn) 
FROM bs_mobile_partner_accounts mpa 
LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id 
LEFT JOIN users u ON u.uid = mpa.uid 
WHERE mpa.uid is not null 
AND mpas.provider_plan != 2
AND mpas.unsubscribed IS NULL 
AND mpas.start < '%s' 
AND mpa.partner = 'tigo' 
AND mpas.current = 1 
group by 1;

-- # TOTAL NUMBER OF NEW TIGO 
SELECT substring(mpa.msisdn, 1,3), count(DISTINCT mpa.uid)
FROM bs_mobile_partner_accounts mpa 
LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id 
LEFT JOIN users u ON u.uid = mpa.uid 
WHERE mpa.uid is not null 
AND mpas.start >= '%s' 
AND mpas.start < '%s' 
AND mpa.partner = 'tigo'
AND mpas.unsubscribed IS NULL 
AND mpas.provider_plan != 2 
AND mpas.current = 1 
group by 1;

-- # TOTAL NUMBER OF tigo USERS REQUESTED TO CANCEL
SELECT substring(mpa.msisdn, 1,3), COUNT(DISTINCT mpa.msisdn) 
FROM bs_mobile_partner_accounts mpa 
LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id 
AND mpas.unsubscribed >= '%s' 
AND mpas.unsubscribed <= '%s' 
AND mpa.partner = 'tigo' 
AND mpas.current = 1 
AND mpas.provider_plan != 2 
group by 1;

-- # TOTAL NUMBER OF tigo users that do no have validated email

SELECT substring(mpa.msisdn, 1,3), count(mpa.msisdn)
FROM bs_mobile_partner_accounts mpa
LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id
WHERE mpas.start < '%s'
AND mpas.unsubscribed IS NULL
AND mpa.uid IS NULL
AND mpa.partner = 'tigo'
AND mpas.current = 1
AND mpas.provider_plan != 2
GROUP BY 1

-- # TOTAL NUMBER OF Tigo USERS THAT HAVE OPENED THE APP
SELECT mpa.uid, mpa.msisdn, mpas.start, mpas.end, mpas.unsubscribed, u.mail
FROM bs_mobile_partner_accounts mpa
LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id
LEFT JOIN users u on u.uid = mpa.uid
WHERE mpas.start <= '%s'
AND mpas.unsubscribed IS NULL
AND mpa.partner = 'tigo'
AND u.access >= '%s'
AND u.access < '%s'
AND mpas.current = 1
AND mpas.provider_plan != 2
GROUP BY mpa.uid;

-- # TOTAL NUMBER OF TIGO USERS THAT HAVE NOT OPENED THE APP
SELECT mpa.uid, mpa.msisdn, mpas.start, mpas.end, mpas.unsubscribed, u.mail
FROM bs_mobile_partner_accounts mpa
LEFT JOIN bs_mobile_partner_accounts_subscriptions mpas ON mpas.mpaid = mpa.id
LEFT JOIN users u on u.uid = mpa.uid
WHERE mpas.start <= '%s'
AND mpas.unsubscribed IS NULL
AND mpa.partner = 'tigo'
AND u.access < '%s'
AND mpas.current = 1
AND mpas.provider_plan != 2
GROUP BY mpa.uid