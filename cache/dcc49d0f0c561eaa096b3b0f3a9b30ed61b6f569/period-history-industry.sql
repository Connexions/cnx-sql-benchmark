-- &query& = period history industry

-- arguments text_terms:string "'period history industry'"
SELECT row_to_json(combined_rows) as results
FROM (

SELECT
  lm.name as title, title_order(lm.name) as "sortTitle",
  lm.uuid as id,
  CASE
    WHEN lm.portal_type = 'Collection'
      THEN lm.major_version || '.' || lm.minor_version
    ELSE lm.major_version || ''
  END AS version,
  language,
  lm.portal_type as "mediaType",
  iso8601(lm.revised) as "pubDate",
  ARRAY(SELECT k.word FROM keywords as k, modulekeywords as mk
        WHERE mk.module_ident = lm.module_ident
              AND mk.keywordid = k.keywordid) as keywords,
  ARRAY(SELECT tags.tag FROM tags, moduletags as mt
        WHERE mt.module_ident = lm.module_ident
              AND mt.tagid = tags.tagid) as subjects,
  ARRAY(SELECT row_to_json(user_rows) FROM
        (SELECT id, email, firstname, othername, surname, fullname,
                title, suffix, website
         FROM users
         WHERE users.id::text = ANY (lm.authors)
         ) as user_rows) as authors,
  -- The following are used internally for further sorting and debugging.
  weight, rank,
  keys as _keys, '' as matched, '' as fields,
  ts_headline(ab.html,plainto_tsquery('period history industry'), 'ShortWord=5, MinWords=50, MaxWords=60') as abstract,
  -- until we actually do something with it
  -- ts_headline(mfti.fulltext, plainto_tsquery('period history industry'),
  --            'StartSel=<b>, StopSel=</b>, ShortWord=5, MinWords=50, MaxWords=60') as headline
  NULL as headline
-- Only retrieve the most recent published modules.
FROM
  latest_modules AS lm 
  NATURAL LEFT JOIN abstracts AS ab 
  NATURAL LEFT JOIN modulefti AS mfti
  
  LEFT OUTER JOIN recent_hit_ranks ON (lm.uuid = document),
  (SELECT
     module_ident,
     cast (sum(weight) as bigint) as weight,
     semilist(keys) as keys
   FROM
     (-- ###
-- Copyright (c) 2013, Rice University
-- This software is subject to the provisions of the GNU Affero General
-- Public License version 3 (AGPLv3).
-- See LICENCE.txt for details.
-- ###
SELECT
  module_ident,
  count(*)*5 as weight,
  semilist(key) as keys
FROM (
  SELECT

  module_ident,

  'period'::text ||'-::-language' as key

FROM

  latest_modules

WHERE

  language ~ ('^'||req('period'::text))

UNION ALL
SELECT

  module_ident,

  'history'::text ||'-::-language' as key

FROM

  latest_modules

WHERE

  language ~ ('^'||req('history'::text))

UNION ALL
SELECT

  module_ident,

  'industry'::text ||'-::-language' as key

FROM

  latest_modules

WHERE

  language ~ ('^'||req('industry'::text))

) cm
GROUP BY cm.module_ident

UNION ALL
-- ###
-- Copyright (c) 2013, Rice University
-- This software is subject to the provisions of the GNU Affero General
-- Public License version 3 (AGPLv3).
-- See LICENCE.txt for details.
-- ###
SELECT
  module_ident,
  count(*)*10 as weight,
  semilist(key) as keys
FROM (
  SELECT

  module_ident,

  'period'::text ||'-::-subject' as key

FROM

  latest_modules NATURAL JOIN moduletags NATURAL JOIN tags

WHERE

  tag ~* req('period'::text)

UNION ALL
SELECT

  module_ident,

  'history'::text ||'-::-subject' as key

FROM

  latest_modules NATURAL JOIN moduletags NATURAL JOIN tags

WHERE

  tag ~* req('history'::text)

UNION ALL
SELECT

  module_ident,

  'industry'::text ||'-::-subject' as key

FROM

  latest_modules NATURAL JOIN moduletags NATURAL JOIN tags

WHERE

  tag ~* req('industry'::text)

) cm
GROUP BY cm.module_ident

UNION ALL
-- ###
-- Copyright (c) 2013, Rice University
-- This software is subject to the provisions of the GNU Affero General
-- Public License version 3 (AGPLv3).
-- See LICENCE.txt for details.
-- ###
SELECT
  module_ident,
  count(*)*1 as weight,
  semilist(key) as keys
FROM (
  SELECT

  cm.module_ident,

  'period'::text ||'-::-fulltext' as key,

  ts_rank_cd('{1.0,1.0,1.0,1.0}', module_idx, plainto_tsquery('period'),4) * 2 ^ length(to_tsvector('period')) as rank

FROM

  latest_modules cm,

  modulefti mf

WHERE

  cm.module_ident = mf.module_ident

  AND

  module_idx @@ plainto_tsquery('period')

UNION ALL
SELECT

  cm.module_ident,

  'history'::text ||'-::-fulltext' as key,

  ts_rank_cd('{1.0,1.0,1.0,1.0}', module_idx, plainto_tsquery('history'),4) * 2 ^ length(to_tsvector('history')) as rank

FROM

  latest_modules cm,

  modulefti mf

WHERE

  cm.module_ident = mf.module_ident

  AND

  module_idx @@ plainto_tsquery('history')

UNION ALL
SELECT

  cm.module_ident,

  'industry'::text ||'-::-fulltext' as key,

  ts_rank_cd('{1.0,1.0,1.0,1.0}', module_idx, plainto_tsquery('industry'),4) * 2 ^ length(to_tsvector('industry')) as rank

FROM

  latest_modules cm,

  modulefti mf

WHERE

  cm.module_ident = mf.module_ident

  AND

  module_idx @@ plainto_tsquery('industry')

) cm
GROUP BY cm.module_ident

UNION ALL
-- ###
-- Copyright (c) 2013, Rice University
-- This software is subject to the provisions of the GNU Affero General
-- Public License version 3 (AGPLv3).
-- See LICENCE.txt for details.
-- ###
SELECT
  module_ident,
  count(*)*1 as weight,
  semilist(key) as keys
FROM (
  SELECT

  cm.module_ident,

  'period'::text ||'-::-abstract' as key

FROM

  latest_modules cm,

  abstracts a

WHERE

  cm.abstractid = a.abstractid

  AND

  a.html ~* req('period'::text)

UNION ALL
SELECT

  cm.module_ident,

  'history'::text ||'-::-abstract' as key

FROM

  latest_modules cm,

  abstracts a

WHERE

  cm.abstractid = a.abstractid

  AND

  a.html ~* req('history'::text)

UNION ALL
SELECT

  cm.module_ident,

  'industry'::text ||'-::-abstract' as key

FROM

  latest_modules cm,

  abstracts a

WHERE

  cm.abstractid = a.abstractid

  AND

  a.html ~* req('industry'::text)

) cm
GROUP BY cm.module_ident

UNION ALL
-- ###
-- Copyright (c) 2013, Rice University
-- This software is subject to the provisions of the GNU Affero General
-- Public License version 3 (AGPLv3).
-- See LICENCE.txt for details.
-- ###
SELECT
  module_ident,
  count(*)*10 as weight,
  semilist(key) as keys
FROM (
  SELECT

  cm.module_ident,

  'period'::text ||'-::-keyword' as key

FROM

  latest_modules cm,

  modulekeywords mk,

  keywords k

WHERE

  cm.module_ident = mk.module_ident

  AND

  mk.keywordid = k.keywordid

  AND

  k.word ~* req('period'::text)

UNION ALL
SELECT

  cm.module_ident,

  'history'::text ||'-::-keyword' as key

FROM

  latest_modules cm,

  modulekeywords mk,

  keywords k

WHERE

  cm.module_ident = mk.module_ident

  AND

  mk.keywordid = k.keywordid

  AND

  k.word ~* req('history'::text)

UNION ALL
SELECT

  cm.module_ident,

  'industry'::text ||'-::-keyword' as key

FROM

  latest_modules cm,

  modulekeywords mk,

  keywords k

WHERE

  cm.module_ident = mk.module_ident

  AND

  mk.keywordid = k.keywordid

  AND

  k.word ~* req('industry'::text)

) cm
GROUP BY cm.module_ident

UNION ALL
-- ###
-- Copyright (c) 2013, Rice University
-- This software is subject to the provisions of the GNU Affero General
-- Public License version 3 (AGPLv3).
-- See LICENCE.txt for details.
-- ###
SELECT
  module_ident,
  count(*)*50 as weight,
  semilist(key) as keys
FROM (
  SELECT

  module_ident,

  'period'::text||'-::-author' as key

FROM

  latest_modules m,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (m.authors)

  AND

  (u.firstname ~* req('period'::text)

   OR

   u.surname ~* req('period'::text)

   OR

   u.fullname ~* req('period'::text)

   OR

   u.email ~* (req('period'::text)||'.*@')

   OR

   (u.email ~* (req('period'::text))

    AND

    'period'::text  ~ '@'

    )

   )

UNION ALL
SELECT

  module_ident,

  'history'::text||'-::-author' as key

FROM

  latest_modules m,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (m.authors)

  AND

  (u.firstname ~* req('history'::text)

   OR

   u.surname ~* req('history'::text)

   OR

   u.fullname ~* req('history'::text)

   OR

   u.email ~* (req('history'::text)||'.*@')

   OR

   (u.email ~* (req('history'::text))

    AND

    'history'::text  ~ '@'

    )

   )

UNION ALL
SELECT

  module_ident,

  'industry'::text||'-::-author' as key

FROM

  latest_modules m,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (m.authors)

  AND

  (u.firstname ~* req('industry'::text)

   OR

   u.surname ~* req('industry'::text)

   OR

   u.fullname ~* req('industry'::text)

   OR

   u.email ~* (req('industry'::text)||'.*@')

   OR

   (u.email ~* (req('industry'::text))

    AND

    'industry'::text  ~ '@'

    )

   )

) cm
GROUP BY cm.module_ident

UNION ALL
-- ###
-- Copyright (c) 2013, Rice University
-- This software is subject to the provisions of the GNU Affero General
-- Public License version 3 (AGPLv3).
-- See LICENCE.txt for details.
-- ###
SELECT
  module_ident,
  count(*)*20 as weight,
  semilist(key) as keys
FROM (
  SELECT

  module_ident,

  'period'::text||'-::-editor' as key

FROM

  latest_modules m

  NATURAL JOIN moduleoptionalroles mor

  NATURAL JOIN roles r,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (mor.personids)

  AND

  lower(r.rolename) = 'editor'

  AND

  (u.firstname ~* req('period'::text)

   OR

   u.surname ~* req('period'::text)

   OR

   u.fullname ~* req('period'::text)

   OR

   u.email ~* (req('period'::text)||'.*@')

   OR

   (u.email ~* (req('period'::text))

    AND

    'period'::text  ~ '@'

    )

   )

UNION ALL
SELECT

  module_ident,

  'history'::text||'-::-editor' as key

FROM

  latest_modules m

  NATURAL JOIN moduleoptionalroles mor

  NATURAL JOIN roles r,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (mor.personids)

  AND

  lower(r.rolename) = 'editor'

  AND

  (u.firstname ~* req('history'::text)

   OR

   u.surname ~* req('history'::text)

   OR

   u.fullname ~* req('history'::text)

   OR

   u.email ~* (req('history'::text)||'.*@')

   OR

   (u.email ~* (req('history'::text))

    AND

    'history'::text  ~ '@'

    )

   )

UNION ALL
SELECT

  module_ident,

  'industry'::text||'-::-editor' as key

FROM

  latest_modules m

  NATURAL JOIN moduleoptionalroles mor

  NATURAL JOIN roles r,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (mor.personids)

  AND

  lower(r.rolename) = 'editor'

  AND

  (u.firstname ~* req('industry'::text)

   OR

   u.surname ~* req('industry'::text)

   OR

   u.fullname ~* req('industry'::text)

   OR

   u.email ~* (req('industry'::text)||'.*@')

   OR

   (u.email ~* (req('industry'::text))

    AND

    'industry'::text  ~ '@'

    )

   )

) cm
GROUP BY cm.module_ident

UNION ALL
-- ###
-- Copyright (c) 2013, Rice University
-- This software is subject to the provisions of the GNU Affero General
-- Public License version 3 (AGPLv3).
-- See LICENCE.txt for details.
-- ###
SELECT
  module_ident,
  count(*)*40 as weight,
  semilist(key) as keys
FROM (
  SELECT

  module_ident,

  'period'::text||'-::-translator' as key

FROM

  latest_modules m

  NATURAL JOIN moduleoptionalroles mor

  NATURAL JOIN roles r,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (mor.personids)

  AND

  lower(r.rolename) = 'translator'

  AND

  (u.firstname ~* req('period'::text)

   OR

   u.surname ~* req('period'::text)

   OR

   u.fullname ~* req('period'::text)

   OR

   u.email ~* (req('period'::text)||'.*@')

   OR

   (u.email ~* (req('period'::text))

    AND

    'period'::text  ~ '@'

    )

   )

UNION ALL
SELECT

  module_ident,

  'history'::text||'-::-translator' as key

FROM

  latest_modules m

  NATURAL JOIN moduleoptionalroles mor

  NATURAL JOIN roles r,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (mor.personids)

  AND

  lower(r.rolename) = 'translator'

  AND

  (u.firstname ~* req('history'::text)

   OR

   u.surname ~* req('history'::text)

   OR

   u.fullname ~* req('history'::text)

   OR

   u.email ~* (req('history'::text)||'.*@')

   OR

   (u.email ~* (req('history'::text))

    AND

    'history'::text  ~ '@'

    )

   )

UNION ALL
SELECT

  module_ident,

  'industry'::text||'-::-translator' as key

FROM

  latest_modules m

  NATURAL JOIN moduleoptionalroles mor

  NATURAL JOIN roles r,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (mor.personids)

  AND

  lower(r.rolename) = 'translator'

  AND

  (u.firstname ~* req('industry'::text)

   OR

   u.surname ~* req('industry'::text)

   OR

   u.fullname ~* req('industry'::text)

   OR

   u.email ~* (req('industry'::text)||'.*@')

   OR

   (u.email ~* (req('industry'::text))

    AND

    'industry'::text  ~ '@'

    )

   )

) cm
GROUP BY cm.module_ident

UNION ALL
-- ###
-- Copyright (c) 2013, Rice University
-- This software is subject to the provisions of the GNU Affero General
-- Public License version 3 (AGPLv3).
-- See LICENCE.txt for details.
-- ###
SELECT
  module_ident,
  count(*)*10 as weight,
  semilist(key) as keys
FROM (
  SELECT

  module_ident,

  'period'::text||'-::-maintainer' as key

FROM

  latest_modules m,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (m.maintainers)

  AND

  (u.firstname ~* req('period'::text)

  OR

  u.surname ~* req('period'::text)

  OR

  u.fullname ~* req('period'::text)

  OR

  u.email ~* (req('period'::text)||'.*@')

  OR

  (u.email ~* (req('period'::text))

   AND

   'period'::text  ~ '@'

   )

  )

UNION ALL
SELECT

  module_ident,

  'history'::text||'-::-maintainer' as key

FROM

  latest_modules m,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (m.maintainers)

  AND

  (u.firstname ~* req('history'::text)

  OR

  u.surname ~* req('history'::text)

  OR

  u.fullname ~* req('history'::text)

  OR

  u.email ~* (req('history'::text)||'.*@')

  OR

  (u.email ~* (req('history'::text))

   AND

   'history'::text  ~ '@'

   )

  )

UNION ALL
SELECT

  module_ident,

  'industry'::text||'-::-maintainer' as key

FROM

  latest_modules m,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (m.maintainers)

  AND

  (u.firstname ~* req('industry'::text)

  OR

  u.surname ~* req('industry'::text)

  OR

  u.fullname ~* req('industry'::text)

  OR

  u.email ~* (req('industry'::text)||'.*@')

  OR

  (u.email ~* (req('industry'::text))

   AND

   'industry'::text  ~ '@'

   )

  )

) cm
GROUP BY cm.module_ident

UNION ALL
-- ###
-- Copyright (c) 2013, Rice University
-- This software is subject to the provisions of the GNU Affero General
-- Public License version 3 (AGPLv3).
-- See LICENCE.txt for details.
-- ###
SELECT
  module_ident,
  count(*)*10 as weight,
  semilist(key) as keys
FROM (
  SELECT

  module_ident,

  'period'::text||'-::-licensor' as key

FROM

  latest_modules m

  NATURAL JOIN moduleoptionalroles mor

  NATURAL JOIN roles r,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (mor.personids)

  AND

  lower(r.rolename) = 'licensor'

  AND

  (u.firstname ~* req('period'::text)

   OR

   u.surname ~* req('period'::text)

   OR

   u.fullname ~* req('period'::text)

   OR

   u.email ~* (req('period'::text)||'.*@')

   OR

   (u.email ~* (req('period'::text))

    AND

    'period'::text  ~ '@'

    )

   )

UNION ALL
SELECT

  module_ident,

  'history'::text||'-::-licensor' as key

FROM

  latest_modules m

  NATURAL JOIN moduleoptionalroles mor

  NATURAL JOIN roles r,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (mor.personids)

  AND

  lower(r.rolename) = 'licensor'

  AND

  (u.firstname ~* req('history'::text)

   OR

   u.surname ~* req('history'::text)

   OR

   u.fullname ~* req('history'::text)

   OR

   u.email ~* (req('history'::text)||'.*@')

   OR

   (u.email ~* (req('history'::text))

    AND

    'history'::text  ~ '@'

    )

   )

UNION ALL
SELECT

  module_ident,

  'industry'::text||'-::-licensor' as key

FROM

  latest_modules m

  NATURAL JOIN moduleoptionalroles mor

  NATURAL JOIN roles r,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (mor.personids)

  AND

  lower(r.rolename) = 'licensor'

  AND

  (u.firstname ~* req('industry'::text)

   OR

   u.surname ~* req('industry'::text)

   OR

   u.fullname ~* req('industry'::text)

   OR

   u.email ~* (req('industry'::text)||'.*@')

   OR

   (u.email ~* (req('industry'::text))

    AND

    'industry'::text  ~ '@'

    )

   )

) cm
GROUP BY cm.module_ident

UNION ALL
-- ###
-- Copyright (c) 2013, Rice University
-- This software is subject to the provisions of the GNU Affero General
-- Public License version 3 (AGPLv3).
-- See LICENCE.txt for details.
-- ###
SELECT
  module_ident,
  count(*)*100 as weight,
  semilist(key) as keys
FROM (
  SELECT

  module_ident,

  'period'::text||'-::-title' as key

FROM

  latest_modules

WHERE

  name ~* ('(^| )'||req('period'::text)||'( |$)'::text)

UNION ALL
SELECT

  module_ident,

  'history'::text||'-::-title' as key

FROM

  latest_modules

WHERE

  name ~* ('(^| )'||req('history'::text)||'( |$)'::text)

UNION ALL
SELECT

  module_ident,

  'industry'::text||'-::-title' as key

FROM

  latest_modules

WHERE

  name ~* ('(^| )'||req('industry'::text)||'( |$)'::text)

) cm
GROUP BY cm.module_ident

UNION ALL
-- ###
-- Copyright (c) 2013, Rice University
-- This software is subject to the provisions of the GNU Affero General
-- Public License version 3 (AGPLv3).
-- See LICENCE.txt for details.
-- ###
SELECT
  module_ident,
  count(*)*10 as weight,
  semilist(key) as keys
FROM (
  SELECT

  module_ident,

  'period'::text||'-::-title' as key

FROM

  latest_modules

WHERE

  name ~* req('period'::text)

UNION ALL
SELECT

  module_ident,

  'history'::text||'-::-title' as key

FROM

  latest_modules

WHERE

  name ~* req('history'::text)

UNION ALL
SELECT

  module_ident,

  'industry'::text||'-::-title' as key

FROM

  latest_modules

WHERE

  name ~* req('industry'::text)

) cm
GROUP BY cm.module_ident
) as matched
   -- table join...
   GROUP BY module_ident
   ) AS weighted
WHERE
  weighted.module_ident = lm.module_ident
  
  

ORDER BY portal_type, weight DESC, uuid DESC

) as combined_rows
;
