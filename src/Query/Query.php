<?php

namespace ClickHouseDB\Query;

use ClickHouseDB\Exception\QueryException;
use function sizeof;

class Query
{
    /**
     * @var string
     */
    protected $sql;

    /**
     * @var string|null
     */
    protected $format = null;

    /**
     * @var array
     */
    private $degenerations = [];

    /**
     * Query constructor.
     * @param string $sql
     * @param array $degenerations
     */
    public function __construct($sql, $degenerations = [])
    {
        if (!trim($sql))
        {
            throw new QueryException('Empty Query');
        }
        $this->sql = $sql;
        $this->degenerations = $degenerations;
    }

    /**
     * @param string|null $format
     */
    public function setFormat($format)
    {
        $this->format = $format;
    }


    private function applyFormatQuery()
    {
        // FORMAT\s(\w)*$
        if (null === $this->format) {
            return false;
        }
        // NOTE: longer FORMAT name should be put to the left-side of the string
        $supportFormats =
            "FORMAT\\s+TabSeparatedWithNamesAndTypes|FORMAT\\s+TabSeparatedWithNames|FORMAT\\s+TabSeparatedRaw|FORMAT\\s+TabSeparated|FORMAT\\s+TSVWithNamesAndTypes|FORMAT\\s+TSVWithNames|FORMAT\\s+TSVRaw|FORMAT\\s+TSV|FORMAT\\s+TSKV|FORMAT\\s+JSONCompact|FORMAT\\s+JSONEachRow|FORMAT\\s+BlockTabSeparated|FORMAT\\s+CSVWithNames|FORMAT\\s+CSV|FORMAT\\s+JSON|FORMAT\\s+Vertical";

        $matches = [];
        if (preg_match_all('%(' . $supportFormats . ')%ius', $this->sql, $matches)) {

            // skip add "format json"
            if (isset($matches[0]))
            {

                $this->format = trim(str_ireplace('format', '', $matches[0][0]));

            }
        } else {
            $this->sql = $this->sql . ' FORMAT ' . $this->format;
        }






    }

    /**
     * @return null|string
     */
    public function getFormat()
    {

        return $this->format;
    }

    public function toSql()
    {
        if ($this->format !== null) {
            $this->applyFormatQuery();
        }

        if (sizeof($this->degenerations))
        {
            foreach ($this->degenerations as $degeneration)
            {
                if ($degeneration instanceof Degeneration) {
                    $this->sql = $degeneration->process($this->sql);
                }
            }
        }

        return $this->sql;
    }

    /**
     * @return string
     */
    public function __toString()
    {
        return $this->toSql();
    }
}
