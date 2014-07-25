-- &query& = science war subject:"Mathematics and Statistics"

-- arguments text_terms:string "'science war'"
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
  ts_headline(ab.html,plainto_tsquery('science war'), 'ShortWord=5, MinWords=50, MaxWords=60') as abstract,
  -- until we actually do something with it
  -- ts_headline(mfti.fulltext, plainto_tsquery('science war'),
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
  count(*)*0 as weight,
  semilist(key) as keys
FROM (
  SELECT

  module_ident,

  'science'::text ||'-::-parentAuthor' as key

FROM

  latest_modules

WHERE

  'science' = any (parentAuthors)

UNION ALL
SELECT

  module_ident,

  'war'::text ||'-::-parentAuthor' as key

FROM

  latest_modules

WHERE

  'war' = any (parentAuthors)

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
  count(*)*0 as weight,
  semilist(key) as keys
FROM (
  SELECT

  module_ident,

  'science'::text ||'-::-language' as key

FROM

  latest_modules

WHERE

  language ~ ('^'||req('science'::text))

UNION ALL
SELECT

  module_ident,

  'war'::text ||'-::-language' as key

FROM

  latest_modules

WHERE

  language ~ ('^'||req('war'::text))

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
  count(*)*0 as weight,
  semilist(key) as keys
FROM (
  SELECT

  module_ident,

  'science'::text ||'-::-subject' as key

FROM

  latest_modules NATURAL JOIN moduletags NATURAL JOIN tags

WHERE

  tag ~* req('science'::text)

UNION ALL
SELECT

  module_ident,

  'war'::text ||'-::-subject' as key

FROM

  latest_modules NATURAL JOIN moduletags NATURAL JOIN tags

WHERE

  tag ~* req('war'::text)

UNION ALL
SELECT

  module_ident,

  'Mathematics and Statistics'::text ||'-::-subject' as key

FROM

  latest_modules NATURAL JOIN moduletags NATURAL JOIN tags

WHERE

  tag ~* req('Mathematics and Statistics'::text)

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
  count(*)*0 as weight,
  semilist(key) as keys
FROM (
  SELECT

  cm.module_ident,

  'science'::text ||'-::-fulltext' as key,

  ts_rank_cd('{1.0,1.0,1.0,1.0}', module_idx, plainto_tsquery('science'),4) * 2 ^ length(to_tsvector('science')) as rank

FROM

  latest_modules cm,

  modulefti mf

WHERE

  cm.module_ident = mf.module_ident

  AND

  module_idx @@ plainto_tsquery('science')

UNION ALL
SELECT

  cm.module_ident,

  'war'::text ||'-::-fulltext' as key,

  ts_rank_cd('{1.0,1.0,1.0,1.0}', module_idx, plainto_tsquery('war'),4) * 2 ^ length(to_tsvector('war')) as rank

FROM

  latest_modules cm,

  modulefti mf

WHERE

  cm.module_ident = mf.module_ident

  AND

  module_idx @@ plainto_tsquery('war')

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
  count(*)*0 as weight,
  semilist(key) as keys
FROM (
  SELECT

  cm.module_ident,

  'science'::text ||'-::-abstract' as key

FROM

  latest_modules cm,

  abstracts a

WHERE

  cm.abstractid = a.abstractid

  AND

  a.html ~* req('science'::text)

UNION ALL
SELECT

  cm.module_ident,

  'war'::text ||'-::-abstract' as key

FROM

  latest_modules cm,

  abstracts a

WHERE

  cm.abstractid = a.abstractid

  AND

  a.html ~* req('war'::text)

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
  count(*)*0 as weight,
  semilist(key) as keys
FROM (
  SELECT

  cm.module_ident,

  'science'::text ||'-::-keyword' as key

FROM

  latest_modules cm,

  modulekeywords mk,

  keywords k

WHERE

  cm.module_ident = mk.module_ident

  AND

  mk.keywordid = k.keywordid

  AND

  k.word ~* req('science'::text)

UNION ALL
SELECT

  cm.module_ident,

  'war'::text ||'-::-keyword' as key

FROM

  latest_modules cm,

  modulekeywords mk,

  keywords k

WHERE

  cm.module_ident = mk.module_ident

  AND

  mk.keywordid = k.keywordid

  AND

  k.word ~* req('war'::text)

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
  count(*)*0 as weight,
  semilist(key) as keys
FROM (
  SELECT

  module_ident,

  'science'::text||'-::-author' as key

FROM

  latest_modules m,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (m.authors)

  AND

  (u.firstname ~* req('science'::text)

   OR

   u.surname ~* req('science'::text)

   OR

   u.fullname ~* req('science'::text)

   OR

   u.email ~* (req('science'::text)||'.*@')

   OR

   (u.email ~* (req('science'::text))

    AND

    'science'::text  ~ '@'

    )

   )

UNION ALL
SELECT

  module_ident,

  'war'::text||'-::-author' as key

FROM

  latest_modules m,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (m.authors)

  AND

  (u.firstname ~* req('war'::text)

   OR

   u.surname ~* req('war'::text)

   OR

   u.fullname ~* req('war'::text)

   OR

   u.email ~* (req('war'::text)||'.*@')

   OR

   (u.email ~* (req('war'::text))

    AND

    'war'::text  ~ '@'

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
  count(*)*0 as weight,
  semilist(key) as keys
FROM (
  SELECT

  module_ident,

  'science'::text||'-::-editor' as key

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

  (u.firstname ~* req('science'::text)

   OR

   u.surname ~* req('science'::text)

   OR

   u.fullname ~* req('science'::text)

   OR

   u.email ~* (req('science'::text)||'.*@')

   OR

   (u.email ~* (req('science'::text))

    AND

    'science'::text  ~ '@'

    )

   )

UNION ALL
SELECT

  module_ident,

  'war'::text||'-::-editor' as key

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

  (u.firstname ~* req('war'::text)

   OR

   u.surname ~* req('war'::text)

   OR

   u.fullname ~* req('war'::text)

   OR

   u.email ~* (req('war'::text)||'.*@')

   OR

   (u.email ~* (req('war'::text))

    AND

    'war'::text  ~ '@'

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
  count(*)*0 as weight,
  semilist(key) as keys
FROM (
  SELECT

  module_ident,

  'science'::text||'-::-translator' as key

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

  (u.firstname ~* req('science'::text)

   OR

   u.surname ~* req('science'::text)

   OR

   u.fullname ~* req('science'::text)

   OR

   u.email ~* (req('science'::text)||'.*@')

   OR

   (u.email ~* (req('science'::text))

    AND

    'science'::text  ~ '@'

    )

   )

UNION ALL
SELECT

  module_ident,

  'war'::text||'-::-translator' as key

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

  (u.firstname ~* req('war'::text)

   OR

   u.surname ~* req('war'::text)

   OR

   u.fullname ~* req('war'::text)

   OR

   u.email ~* (req('war'::text)||'.*@')

   OR

   (u.email ~* (req('war'::text))

    AND

    'war'::text  ~ '@'

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
  count(*)*0 as weight,
  semilist(key) as keys
FROM (
  SELECT

  module_ident,

  'science'::text||'-::-maintainer' as key

FROM

  latest_modules m,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (m.maintainers)

  AND

  (u.firstname ~* req('science'::text)

  OR

  u.surname ~* req('science'::text)

  OR

  u.fullname ~* req('science'::text)

  OR

  u.email ~* (req('science'::text)||'.*@')

  OR

  (u.email ~* (req('science'::text))

   AND

   'science'::text  ~ '@'

   )

  )

UNION ALL
SELECT

  module_ident,

  'war'::text||'-::-maintainer' as key

FROM

  latest_modules m,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (m.maintainers)

  AND

  (u.firstname ~* req('war'::text)

  OR

  u.surname ~* req('war'::text)

  OR

  u.fullname ~* req('war'::text)

  OR

  u.email ~* (req('war'::text)||'.*@')

  OR

  (u.email ~* (req('war'::text))

   AND

   'war'::text  ~ '@'

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
  count(*)*0 as weight,
  semilist(key) as keys
FROM (
  SELECT

  module_ident,

  'science'::text||'-::-licensor' as key

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

  (u.firstname ~* req('science'::text)

   OR

   u.surname ~* req('science'::text)

   OR

   u.fullname ~* req('science'::text)

   OR

   u.email ~* (req('science'::text)||'.*@')

   OR

   (u.email ~* (req('science'::text))

    AND

    'science'::text  ~ '@'

    )

   )

UNION ALL
SELECT

  module_ident,

  'war'::text||'-::-licensor' as key

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

  (u.firstname ~* req('war'::text)

   OR

   u.surname ~* req('war'::text)

   OR

   u.fullname ~* req('war'::text)

   OR

   u.email ~* (req('war'::text)||'.*@')

   OR

   (u.email ~* (req('war'::text))

    AND

    'war'::text  ~ '@'

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
  count(*)*0 as weight,
  semilist(key) as keys
FROM (
  SELECT

  module_ident,

  'science'::text||'-::-title' as key

FROM

  latest_modules

WHERE

  name ~* ('(^| )'||req('science'::text)||'( |$)'::text)

UNION ALL
SELECT

  module_ident,

  'war'::text||'-::-title' as key

FROM

  latest_modules

WHERE

  name ~* ('(^| )'||req('war'::text)||'( |$)'::text)

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
  count(*)*0 as weight,
  semilist(key) as keys
FROM (
  SELECT

  module_ident,

  'science'::text||'-::-title' as key

FROM

  latest_modules

WHERE

  name ~* req('science'::text)

UNION ALL
SELECT

  module_ident,

  'war'::text||'-::-title' as key

FROM

  latest_modules

WHERE

  name ~* req('war'::text)

) cm
GROUP BY cm.module_ident
) as matched
   -- table join...
   GROUP BY module_ident
   ) AS weighted
WHERE
  weighted.module_ident = lm.module_ident
  
  

ORDER BY weight DESC, uuid DESC

) as combined_rows
;
