-- &query& = music michael instrument

-- arguments text_terms:string "'music michael instrument'"
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
  ts_headline(ab.html,plainto_tsquery('music michael instrument'), 'ShortWord=5, MinWords=50, MaxWords=60') as abstract,
  -- until we actually do something with it
  -- ts_headline(mfti.fulltext, plainto_tsquery('music michael instrument'),
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

  'music'::text ||'-::-parentAuthor' as key

FROM

  latest_modules

WHERE

  'music' = any (parentAuthors)

UNION ALL
SELECT

  module_ident,

  'michael'::text ||'-::-parentAuthor' as key

FROM

  latest_modules

WHERE

  'michael' = any (parentAuthors)

UNION ALL
SELECT

  module_ident,

  'instrument'::text ||'-::-parentAuthor' as key

FROM

  latest_modules

WHERE

  'instrument' = any (parentAuthors)

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

  'music'::text ||'-::-language' as key

FROM

  latest_modules

WHERE

  language ~ ('^'||req('music'::text))

UNION ALL
SELECT

  module_ident,

  'michael'::text ||'-::-language' as key

FROM

  latest_modules

WHERE

  language ~ ('^'||req('michael'::text))

UNION ALL
SELECT

  module_ident,

  'instrument'::text ||'-::-language' as key

FROM

  latest_modules

WHERE

  language ~ ('^'||req('instrument'::text))

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

  'music'::text ||'-::-subject' as key

FROM

  latest_modules NATURAL JOIN moduletags NATURAL JOIN tags

WHERE

  tag ~* req('music'::text)

UNION ALL
SELECT

  module_ident,

  'michael'::text ||'-::-subject' as key

FROM

  latest_modules NATURAL JOIN moduletags NATURAL JOIN tags

WHERE

  tag ~* req('michael'::text)

UNION ALL
SELECT

  module_ident,

  'instrument'::text ||'-::-subject' as key

FROM

  latest_modules NATURAL JOIN moduletags NATURAL JOIN tags

WHERE

  tag ~* req('instrument'::text)

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

  'music'::text ||'-::-fulltext' as key,

  ts_rank_cd('{1.0,1.0,1.0,1.0}', module_idx, plainto_tsquery('music'),4) * 2 ^ length(to_tsvector('music')) as rank

FROM

  latest_modules cm,

  modulefti mf

WHERE

  cm.module_ident = mf.module_ident

  AND

  module_idx @@ plainto_tsquery('music')

UNION ALL
SELECT

  cm.module_ident,

  'michael'::text ||'-::-fulltext' as key,

  ts_rank_cd('{1.0,1.0,1.0,1.0}', module_idx, plainto_tsquery('michael'),4) * 2 ^ length(to_tsvector('michael')) as rank

FROM

  latest_modules cm,

  modulefti mf

WHERE

  cm.module_ident = mf.module_ident

  AND

  module_idx @@ plainto_tsquery('michael')

UNION ALL
SELECT

  cm.module_ident,

  'instrument'::text ||'-::-fulltext' as key,

  ts_rank_cd('{1.0,1.0,1.0,1.0}', module_idx, plainto_tsquery('instrument'),4) * 2 ^ length(to_tsvector('instrument')) as rank

FROM

  latest_modules cm,

  modulefti mf

WHERE

  cm.module_ident = mf.module_ident

  AND

  module_idx @@ plainto_tsquery('instrument')

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

  'music'::text ||'-::-abstract' as key

FROM

  latest_modules cm,

  abstracts a

WHERE

  cm.abstractid = a.abstractid

  AND

  a.html ~* req('music'::text)

UNION ALL
SELECT

  cm.module_ident,

  'michael'::text ||'-::-abstract' as key

FROM

  latest_modules cm,

  abstracts a

WHERE

  cm.abstractid = a.abstractid

  AND

  a.html ~* req('michael'::text)

UNION ALL
SELECT

  cm.module_ident,

  'instrument'::text ||'-::-abstract' as key

FROM

  latest_modules cm,

  abstracts a

WHERE

  cm.abstractid = a.abstractid

  AND

  a.html ~* req('instrument'::text)

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

  'music'::text ||'-::-keyword' as key

FROM

  latest_modules cm,

  modulekeywords mk,

  keywords k

WHERE

  cm.module_ident = mk.module_ident

  AND

  mk.keywordid = k.keywordid

  AND

  k.word ~* req('music'::text)

UNION ALL
SELECT

  cm.module_ident,

  'michael'::text ||'-::-keyword' as key

FROM

  latest_modules cm,

  modulekeywords mk,

  keywords k

WHERE

  cm.module_ident = mk.module_ident

  AND

  mk.keywordid = k.keywordid

  AND

  k.word ~* req('michael'::text)

UNION ALL
SELECT

  cm.module_ident,

  'instrument'::text ||'-::-keyword' as key

FROM

  latest_modules cm,

  modulekeywords mk,

  keywords k

WHERE

  cm.module_ident = mk.module_ident

  AND

  mk.keywordid = k.keywordid

  AND

  k.word ~* req('instrument'::text)

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

  'music'::text||'-::-author' as key

FROM

  latest_modules m,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (m.authors)

  AND

  (u.firstname ~* req('music'::text)

   OR

   u.surname ~* req('music'::text)

   OR

   u.fullname ~* req('music'::text)

   OR

   u.email ~* (req('music'::text)||'.*@')

   OR

   (u.email ~* (req('music'::text))

    AND

    'music'::text  ~ '@'

    )

   )

UNION ALL
SELECT

  module_ident,

  'michael'::text||'-::-author' as key

FROM

  latest_modules m,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (m.authors)

  AND

  (u.firstname ~* req('michael'::text)

   OR

   u.surname ~* req('michael'::text)

   OR

   u.fullname ~* req('michael'::text)

   OR

   u.email ~* (req('michael'::text)||'.*@')

   OR

   (u.email ~* (req('michael'::text))

    AND

    'michael'::text  ~ '@'

    )

   )

UNION ALL
SELECT

  module_ident,

  'instrument'::text||'-::-author' as key

FROM

  latest_modules m,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (m.authors)

  AND

  (u.firstname ~* req('instrument'::text)

   OR

   u.surname ~* req('instrument'::text)

   OR

   u.fullname ~* req('instrument'::text)

   OR

   u.email ~* (req('instrument'::text)||'.*@')

   OR

   (u.email ~* (req('instrument'::text))

    AND

    'instrument'::text  ~ '@'

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

  'music'::text||'-::-editor' as key

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

  (u.firstname ~* req('music'::text)

   OR

   u.surname ~* req('music'::text)

   OR

   u.fullname ~* req('music'::text)

   OR

   u.email ~* (req('music'::text)||'.*@')

   OR

   (u.email ~* (req('music'::text))

    AND

    'music'::text  ~ '@'

    )

   )

UNION ALL
SELECT

  module_ident,

  'michael'::text||'-::-editor' as key

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

  (u.firstname ~* req('michael'::text)

   OR

   u.surname ~* req('michael'::text)

   OR

   u.fullname ~* req('michael'::text)

   OR

   u.email ~* (req('michael'::text)||'.*@')

   OR

   (u.email ~* (req('michael'::text))

    AND

    'michael'::text  ~ '@'

    )

   )

UNION ALL
SELECT

  module_ident,

  'instrument'::text||'-::-editor' as key

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

  (u.firstname ~* req('instrument'::text)

   OR

   u.surname ~* req('instrument'::text)

   OR

   u.fullname ~* req('instrument'::text)

   OR

   u.email ~* (req('instrument'::text)||'.*@')

   OR

   (u.email ~* (req('instrument'::text))

    AND

    'instrument'::text  ~ '@'

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

  'music'::text||'-::-translator' as key

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

  (u.firstname ~* req('music'::text)

   OR

   u.surname ~* req('music'::text)

   OR

   u.fullname ~* req('music'::text)

   OR

   u.email ~* (req('music'::text)||'.*@')

   OR

   (u.email ~* (req('music'::text))

    AND

    'music'::text  ~ '@'

    )

   )

UNION ALL
SELECT

  module_ident,

  'michael'::text||'-::-translator' as key

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

  (u.firstname ~* req('michael'::text)

   OR

   u.surname ~* req('michael'::text)

   OR

   u.fullname ~* req('michael'::text)

   OR

   u.email ~* (req('michael'::text)||'.*@')

   OR

   (u.email ~* (req('michael'::text))

    AND

    'michael'::text  ~ '@'

    )

   )

UNION ALL
SELECT

  module_ident,

  'instrument'::text||'-::-translator' as key

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

  (u.firstname ~* req('instrument'::text)

   OR

   u.surname ~* req('instrument'::text)

   OR

   u.fullname ~* req('instrument'::text)

   OR

   u.email ~* (req('instrument'::text)||'.*@')

   OR

   (u.email ~* (req('instrument'::text))

    AND

    'instrument'::text  ~ '@'

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

  'music'::text||'-::-maintainer' as key

FROM

  latest_modules m,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (m.maintainers)

  AND

  (u.firstname ~* req('music'::text)

  OR

  u.surname ~* req('music'::text)

  OR

  u.fullname ~* req('music'::text)

  OR

  u.email ~* (req('music'::text)||'.*@')

  OR

  (u.email ~* (req('music'::text))

   AND

   'music'::text  ~ '@'

   )

  )

UNION ALL
SELECT

  module_ident,

  'michael'::text||'-::-maintainer' as key

FROM

  latest_modules m,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (m.maintainers)

  AND

  (u.firstname ~* req('michael'::text)

  OR

  u.surname ~* req('michael'::text)

  OR

  u.fullname ~* req('michael'::text)

  OR

  u.email ~* (req('michael'::text)||'.*@')

  OR

  (u.email ~* (req('michael'::text))

   AND

   'michael'::text  ~ '@'

   )

  )

UNION ALL
SELECT

  module_ident,

  'instrument'::text||'-::-maintainer' as key

FROM

  latest_modules m,

  users u

WHERE

  -- FIXME Casting user.id to text. Shouldn't need to do this.

  u.id::text = any (m.maintainers)

  AND

  (u.firstname ~* req('instrument'::text)

  OR

  u.surname ~* req('instrument'::text)

  OR

  u.fullname ~* req('instrument'::text)

  OR

  u.email ~* (req('instrument'::text)||'.*@')

  OR

  (u.email ~* (req('instrument'::text))

   AND

   'instrument'::text  ~ '@'

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

  'music'::text||'-::-licensor' as key

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

  (u.firstname ~* req('music'::text)

   OR

   u.surname ~* req('music'::text)

   OR

   u.fullname ~* req('music'::text)

   OR

   u.email ~* (req('music'::text)||'.*@')

   OR

   (u.email ~* (req('music'::text))

    AND

    'music'::text  ~ '@'

    )

   )

UNION ALL
SELECT

  module_ident,

  'michael'::text||'-::-licensor' as key

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

  (u.firstname ~* req('michael'::text)

   OR

   u.surname ~* req('michael'::text)

   OR

   u.fullname ~* req('michael'::text)

   OR

   u.email ~* (req('michael'::text)||'.*@')

   OR

   (u.email ~* (req('michael'::text))

    AND

    'michael'::text  ~ '@'

    )

   )

UNION ALL
SELECT

  module_ident,

  'instrument'::text||'-::-licensor' as key

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

  (u.firstname ~* req('instrument'::text)

   OR

   u.surname ~* req('instrument'::text)

   OR

   u.fullname ~* req('instrument'::text)

   OR

   u.email ~* (req('instrument'::text)||'.*@')

   OR

   (u.email ~* (req('instrument'::text))

    AND

    'instrument'::text  ~ '@'

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

  'music'::text||'-::-title' as key

FROM

  latest_modules

WHERE

  name ~* ('(^| )'||req('music'::text)||'( |$)'::text)

UNION ALL
SELECT

  module_ident,

  'michael'::text||'-::-title' as key

FROM

  latest_modules

WHERE

  name ~* ('(^| )'||req('michael'::text)||'( |$)'::text)

UNION ALL
SELECT

  module_ident,

  'instrument'::text||'-::-title' as key

FROM

  latest_modules

WHERE

  name ~* ('(^| )'||req('instrument'::text)||'( |$)'::text)

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

  'music'::text||'-::-title' as key

FROM

  latest_modules

WHERE

  name ~* req('music'::text)

UNION ALL
SELECT

  module_ident,

  'michael'::text||'-::-title' as key

FROM

  latest_modules

WHERE

  name ~* req('michael'::text)

UNION ALL
SELECT

  module_ident,

  'instrument'::text||'-::-title' as key

FROM

  latest_modules

WHERE

  name ~* req('instrument'::text)

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
